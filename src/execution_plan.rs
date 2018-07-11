use persistence::*;
use persistence::Persistence;
use search::add_boost;
use search::*;
use search::{Request, RequestBoostPart, RequestSearchPart, SearchError};
use search_field;
use util;
use util::StringAdd;

use crossbeam_channel;
use crossbeam_channel::unbounded;
use search_field::*;

type PlanDataSender = crossbeam_channel::Sender<SearchFieldResult>;
type PlanDataReceiver = crossbeam_channel::Receiver<SearchFieldResult>;

#[derive(Clone, Debug)]
pub enum PlanStepType {
    FieldSearch {
        req: RequestSearchPart,
        input_prev_steps: Vec<PlanDataReceiver>,
        output_next_steps: PlanDataSender,
        plans_output: PlanDataReceiver,
    },
    ValueIdToParent {
        path: String,
        trace_info: String,
        input_prev_steps: Vec<PlanDataReceiver>,
        output_next_steps: PlanDataSender,
        plans_output: PlanDataReceiver,
    },
    Boost {
        req: RequestBoostPart,
        input_prev_steps: Vec<PlanDataReceiver>,
        output_next_steps: PlanDataSender,
        plans_output: PlanDataReceiver,
    },
    Union {
        steps: Vec<PlanStepType>,
        input_prev_steps: Vec<PlanDataReceiver>,
        output_next_steps: PlanDataSender,
        plans_output: PlanDataReceiver,
    },
    Intersect {
        steps: Vec<PlanStepType>,
        input_prev_steps: Vec<PlanDataReceiver>,
        output_next_steps: PlanDataSender,
        plans_output: PlanDataReceiver,
    },
    FromAttribute {
        steps: Vec<PlanStepType>,
        output_next_steps: PlanDataReceiver,
        plans_output: PlanDataReceiver,
    },
}

pub trait OutputProvider {
    fn get_output(&self) -> PlanDataReceiver;
}

impl OutputProvider for PlanStepType {
    fn get_output(&self) -> PlanDataReceiver {
        match *self {
            PlanStepType::FieldSearch { ref plans_output, .. }
            | PlanStepType::ValueIdToParent { ref plans_output, .. }
            | PlanStepType::Boost { ref plans_output, .. }
            | PlanStepType::Union { ref plans_output, .. }
            | PlanStepType::Intersect { ref plans_output, .. }
            | PlanStepType::FromAttribute { ref plans_output, .. } => plans_output.clone(),
        }
    }
}

fn get_data(input_prev_steps: Vec<PlanDataReceiver>) -> Result<Vec<SearchFieldResult>, SearchError> {
    let mut dat = vec![];
    for el in input_prev_steps {
        dat.push(el.recv()?);
        drop(el);
    }
    Ok(dat)
}

pub trait StepExecutor {
    fn execute_step(self, persistence: &Persistence) -> Result<(), SearchError>;
}

impl StepExecutor for PlanStepType {
    #[allow(unused_variables)]
    #[cfg_attr(feature = "flame_it", flame)]
    fn execute_step(self, persistence: &Persistence) -> Result<(), SearchError> {
        match self {
            PlanStepType::FieldSearch {
                req,
                input_prev_steps,
                output_next_steps,
                ..
            } => {
                let field_result = search_field::get_hits_in_field(persistence, &req, None)?;
                output_next_steps.send(field_result)?;
                drop(output_next_steps);
                for el in input_prev_steps {
                    drop(el);
                }
                // Ok(field_result.hits)
                Ok(())
            }
            PlanStepType::ValueIdToParent {
                input_prev_steps,
                output_next_steps,
                path,
                trace_info: joop,
                ..
            } => {
                output_next_steps.send(join_to_parent_with_score(persistence, &input_prev_steps[0].recv()?, &path, &joop)?)?;
                for el in input_prev_steps {
                    drop(el);
                }
                drop(output_next_steps);
                Ok(())
            }
            PlanStepType::Boost {
                req,
                input_prev_steps,
                output_next_steps,
                ..
            } => {
                let mut input = input_prev_steps[0].recv()?;
                add_boost(persistence, &req, &mut input)?;
                output_next_steps.send(input)?;
                for el in input_prev_steps {
                    drop(el);
                }
                drop(output_next_steps);
                Ok(())
            }
            PlanStepType::Union {
                steps,
                input_prev_steps,
                output_next_steps,
                ..
            } => {
                debug_time!("union total");
                execute_steps(steps, persistence)?;
                debug_time!("union netto");
                output_next_steps.send(union_hits_vec(get_data(input_prev_steps)?))?;
                drop(output_next_steps);
                Ok(())
            }
            PlanStepType::Intersect {
                steps,
                input_prev_steps,
                output_next_steps,
                ..
            } => {
                debug_time!("intersect total");
                execute_steps(steps, persistence)?;
                debug_time!("intersect netto");
                output_next_steps.send(intersect_hits_vec(get_data(input_prev_steps)?))?;
                drop(output_next_steps);
                Ok(())
            }
            PlanStepType::FromAttribute { steps, .. } => {
                execute_steps(steps, persistence)?;
                // output_next_steps.send(intersect_hits(input_prev_steps.iter().map(|el| el.recv().unwrap()).collect()));
                // drop(output_next_steps);
                Ok(())
            }
        }
    }
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn plan_creator(request: Request) -> PlanStepType {
    let (tx, rx): (PlanDataSender, PlanDataReceiver) = unbounded();

    if let Some(or) = request.or {
        let steps: Vec<PlanStepType> = or.iter().map(|x| plan_creator(x.clone())).collect();
        let result_channels_from_prev_steps = steps.iter().map(|el| el.get_output()).collect();
        PlanStepType::Union {
            steps,
            input_prev_steps: result_channels_from_prev_steps,
            output_next_steps: tx,
            plans_output: rx,
        }
    } else if let Some(ands) = request.and {
        let steps: Vec<PlanStepType> = ands.iter().map(|x| plan_creator(x.clone())).collect();
        let result_channels_from_prev_steps = steps.iter().map(|el| el.get_output()).collect();
        PlanStepType::Intersect {
            steps,
            input_prev_steps: result_channels_from_prev_steps,
            output_next_steps: tx,
            plans_output: rx,
        }
    } else if let Some(part) = request.search.clone() {
        // TODO Tokenize query according to field
        // part.terms = part.terms.iter().map(|el| util::normalize_text(el)).collect::<Vec<_>>();
        plan_creator_search_part(part, request)
    } else {
        //TODO HANDLE SUGGEST
        //TODO ADD ERROR
        // plan_creator_search_part(request.search.as_ref().unwrap().clone(), request)
        panic!("missing 'and' 'or' 'search' in request - suggest not yet handled in search api {:?}", request);
    }
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn plan_creator_search_part(mut request_part: RequestSearchPart, mut request: Request) -> PlanStepType {
    let paths = util::get_steps_to_anchor(&request_part.path);

    let (field_tx, field_rx): (PlanDataSender, PlanDataReceiver) = unbounded();

    request_part.fast_field = request.boost.is_none() && !request_part.snippet.unwrap_or(false); // fast_field disabled for boosting or _highlighting_ currently

    request_part.store_term_id_hits = request.why_found || request.text_locality;

    request_part.store_term_texts = request.why_found;

    if request_part.fast_field {
        PlanStepType::FieldSearch {
            plans_output: field_rx.clone(),
            req: request_part,
            input_prev_steps: vec![],
            output_next_steps: field_tx,
        }
    } else {
        let mut steps = vec![];
        //search in fields
        steps.push(PlanStepType::FieldSearch {
            plans_output: field_rx.clone(),
            req: request_part,
            input_prev_steps: vec![],
            output_next_steps: field_tx,
        });

        let (mut tx, mut rx): (PlanDataSender, PlanDataReceiver) = unbounded();

        steps.push(PlanStepType::ValueIdToParent {
            plans_output: rx.clone(),
            input_prev_steps: vec![field_rx],
            output_next_steps: tx.clone(),
            path: paths.last().unwrap().add(VALUE_ID_TO_PARENT),
            trace_info: "term hits hit to column".to_string(),
        });

        for i in (0..paths.len() - 1).rev() {
            if request.boost.is_some() {
                request.boost.as_mut().unwrap().retain(|boost| {
                    let apply_boost = boost.path.starts_with(&paths[i]);
                    if apply_boost {
                        let (next_tx, next_rx): (PlanDataSender, PlanDataReceiver) = unbounded();
                        tx = next_tx;
                        steps.push(PlanStepType::Boost {
                            plans_output: next_rx.clone(),
                            req: boost.clone(),
                            input_prev_steps: vec![rx.clone()],
                            output_next_steps: tx.clone(),
                        });

                        debug!("PlanCreator Step {:?}", boost);

                        rx = next_rx;
                    }
                    !apply_boost
                });
            }

            let (next_tx, next_rx): (PlanDataSender, PlanDataReceiver) = unbounded();
            tx = next_tx;

            steps.push(PlanStepType::ValueIdToParent {
                plans_output: next_rx.clone(),
                input_prev_steps: vec![rx.clone()],
                output_next_steps: tx.clone(),
                path: paths[i].add(VALUE_ID_TO_PARENT),
                trace_info: "Joining to anchor".to_string(),
            });

            debug!("PlanCreator Step {}", paths[i].add(VALUE_ID_TO_PARENT));

            rx = next_rx;
        }

        if let Some(boosts) = request.boost {
            // Handling boost from anchor to value - TODO FIXME Error when 1:N
            for boost in boosts {
                let (next_tx, next_rx): (PlanDataSender, PlanDataReceiver) = unbounded();
                tx = next_tx;
                steps.push(PlanStepType::Boost {
                    plans_output: next_rx.clone(),
                    req: boost.clone(),
                    input_prev_steps: vec![rx.clone()],
                    output_next_steps: tx.clone(),
                });
                debug!("PlanCreator Step {:?}", boost);
                rx = next_rx;
            }
        }

        PlanStepType::FromAttribute {
            plans_output: rx.clone(),
            steps,
            output_next_steps: rx,
        }
    }

    // (steps, rx)
}

use rayon::prelude::*;

#[cfg_attr(feature = "flame_it", flame)]
pub fn execute_steps(steps: Vec<PlanStepType>, persistence: &Persistence) -> Result<(), SearchError> {
    let r: Result<Vec<_>, SearchError> = steps.into_par_iter().map(|step| step.execute_step(persistence)).collect();

    if r.is_err() {
        Err(r.unwrap_err())
    } else {
        Ok(())
    }

    // for step in steps {
    //     step.execute_step(persistence)?;
    // }
    // Ok(())

    // let err = steps.par_iter().map(|step|{
    //     let res = execute_step(step.clone(), persistence);

    //     match res {
    //         Ok(()) => Some(1),
    //         Err(err)=> None,
    //     }

    // }).while_some().collect::<Vec<_>>();

    // err

    // steps.par_iter().map(|step|{
    //     execute_step(step.clone(), persistence)?;
    // });

    // for step in steps {
    //     execute_step(step, persistence)?;
    // }
    // Ok(())
    // Ok(hits)
}
