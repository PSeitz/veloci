use std::fmt::Debug;
use persistence::Persistence;
use persistence::*;
use search::add_boost;
use search::*;
use search::{Request, RequestBoostPart, RequestSearchPart, SearchError};
use search_field;
use util;
use util::StringAdd;

use crossbeam_channel;
use crossbeam_channel::unbounded;
use search_field::*;

#[derive(Debug, Clone, Copy)]
pub struct Dependency {
    step_index: usize,
    depends_on: usize,
}

#[derive(Debug, Clone)]
pub struct Plan<T: Clone + Debug> {
    pub steps: Vec<T>,
    dependencies: Vec<Dependency>
}

impl<T: Clone + Debug + PartialEq> Default for Plan<T> {
    fn default() -> Plan<T> {
        Plan { steps: vec![], dependencies: vec![] }
    }
}

impl<T: Clone + Debug + PartialEq> Plan<T> {
    fn add_dependency(&mut self, step:&T, depends_on: &T) {
        let step_index = self.steps.iter().position(|el| el == step).unwrap();
        let depends_on = self.steps.iter().position(|el| el == depends_on).unwrap();
        self.dependencies.push(Dependency{step_index, depends_on});
    }
    fn add_step(&mut self, step:T) -> &mut T {
        self.steps.push(step);
        self.steps.last_mut().unwrap()
        // self.steps.len() - 1
    }

    fn get_dependencies(&self, step_index: usize) -> Vec<Dependency> {
        self.dependencies.iter().filter(|dep|dep.step_index == step_index).cloned().collect()
    }

    pub fn get_ordered_steps(&self) -> Vec<Vec<T>> {
        let mut ordered_steps = vec![];
        let mut remaining_steps:Vec<_> = self.steps.iter().enumerate().collect();

        while !remaining_steps.is_empty() {
            let current_remaining_steps = remaining_steps.clone();
            let steps_with_fullfilled_dependencies: Vec<_> = remaining_steps.drain_filter(|step_with_index| {
                let steps_dependencies = self.get_dependencies(step_with_index.0);
                let unfulfilled_dependencies:Vec<_> = steps_dependencies.iter().filter(|dep|{
                    current_remaining_steps.iter().any(|step_with_index| step_with_index.0 == dep.depends_on) // check if depends_on is in current_remaining_steps
                }).collect();

                unfulfilled_dependencies.is_empty()
            }).collect();

            ordered_steps.push(steps_with_fullfilled_dependencies.iter().map(|step_with_index|step_with_index.1.clone()).collect());
        }

        ordered_steps
    }
}

#[test]
fn test_plan() {
    let plan = Plan::<String>{
        steps: vec!["suche_feld".to_string(), "oder".to_string()],
        dependencies: vec![Dependency{step_index: 1, depends_on: 0}]
    };
    let steps = plan.get_ordered_steps();
    assert_eq!(steps[0], vec!["suche_feld"]);
    assert_eq!(steps[1], vec!["oder"]);

}


#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
pub struct PlanRequestSearchPart {
    pub request: RequestSearchPart,

    #[serde(default)]
    pub ids_only: bool,

    /// Internal data
    #[serde(skip_deserializing)]
    #[serde(default)]
    pub fast_field: bool,

    /// Internal data used for whyfound - read and highlight fields
    #[serde(skip_deserializing)]
    #[serde(default)]
    pub store_term_id_hits: bool,

    /// Internal data used for whyfound - highlight in original document
    #[serde(skip_deserializing)]
    #[serde(default)]
    pub store_term_texts: bool,

    /// Also return the actual text
    #[serde(skip_serializing_if = "skip_false")]
    pub return_term: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolve_token_to_parent_hits: Option<bool>,

}

type PlanDataSender = crossbeam_channel::Sender<SearchFieldResult>;
type PlanDataReceiver = crossbeam_channel::Receiver<SearchFieldResult>;

#[derive(Debug, Clone, PartialEq)]
pub struct PlanStepDataChannels {
    input_prev_steps: Vec<PlanDataReceiver>,
    output_sending_to_next_steps: PlanDataSender,
    plans_output_receiver_for_next_step: PlanDataReceiver, // used in plan_creation
}

#[derive(Clone, Debug, PartialEq)]
pub enum PlanStepType {
    FieldSearchToTermIds {
        req: PlanRequestSearchPart,
        channels: PlanStepDataChannels,
    },
    FieldSearchAndAnchorResolve {
        req: PlanRequestSearchPart,
        channels: PlanStepDataChannels,
    },
    ValueIdToParent {
        path: String,
        trace_info: String,
        channels: PlanStepDataChannels,
    },
    Boost {
        req: RequestBoostPart,
        channels: PlanStepDataChannels,
    },
    Union {
        steps: Vec<PlanStepType>,
        channels: PlanStepDataChannels,
    },
    Intersect {
        steps: Vec<PlanStepType>,
        channels: PlanStepDataChannels,
    },
    FromAttribute {
        steps: Vec<PlanStepType>,
        channels: PlanStepDataChannels,
    },
}

// impl PartialEq for PlanStepType {
//     fn eq(&self, other: &PlanStepType) -> bool {
//         false
//         // match *self {
//         //     PlanStepType::FieldSearchAndAnchorResolve { ref channels, .. }
//         //     | PlanStepType::FieldSearchToTermIds { ref channels, .. }
//         //     | PlanStepType::ValueIdToParent { ref channels, .. }
//         //     | PlanStepType::Boost { ref channels, .. }
//         //     | PlanStepType::Union { ref channels, .. }
//         //     | PlanStepType::Intersect { ref channels, .. }
//         //     | PlanStepType::FromAttribute { ref channels, .. } => channels.plans_output_receiver_for_next_step.clone(),
//         // }
//     }
// }

// impl fmt::Debug for PlanStepType {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {

//         match *self {
//             PlanStepType::FieldSearchAndAnchorResolve { ref channels, .. }
//             | PlanStepType::FieldSearchToTermIds { ref channels, .. }
//             | PlanStepType::ValueIdToParent { ref channels, .. }
//             | PlanStepType::Boost { ref channels, .. }
//             | PlanStepType::Union { ref channels, .. }
//             | PlanStepType::Intersect { ref channels, .. }
//             | PlanStepType::FromAttribute { ref channels, .. } => channels.plans_output_receiver_for_next_step.clone(),
//         }

//         write!(f, "Point {{ x: {}, y: {} }}", self.x, self.y)
//     }
// }

pub trait OutputProvider {
    fn get_output(&self) -> PlanDataReceiver;
}

impl OutputProvider for PlanStepType {
    fn get_output(&self) -> PlanDataReceiver {
        match *self {
            PlanStepType::FieldSearchAndAnchorResolve { ref channels, .. }
            | PlanStepType::FieldSearchToTermIds { ref channels, .. }
            | PlanStepType::ValueIdToParent { ref channels, .. }
            | PlanStepType::Boost { ref channels, .. }
            | PlanStepType::Union { ref channels, .. }
            | PlanStepType::Intersect { ref channels, .. }
            | PlanStepType::FromAttribute { ref channels, .. } => channels.plans_output_receiver_for_next_step.clone(),
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
            PlanStepType::FieldSearchToTermIds {
                req,
                channels,
                ..
            } => {
                let field_result = search_field::get_hits_in_field(persistence, &req, None)?;
                let mut data = vec![field_result];
                for _ in 0..1 {
                    let clone = data[0].clone();
                    data.push(clone);
                    // output_sending_to_next_steps.send(field_result)?;
                }
                for el in data {
                    channels.output_sending_to_next_steps.send(el)?;
                }
                drop(channels.output_sending_to_next_steps);
                for el in channels.input_prev_steps {
                    drop(el);
                }
                Ok(())
            }
            PlanStepType::FieldSearchAndAnchorResolve {
                req,
                channels,
                ..
            } => {
                let field_result = search_field::get_hits_in_field(persistence, &req, None)?;
                channels.output_sending_to_next_steps.send(field_result)?;
                drop(channels.output_sending_to_next_steps);
                for el in channels.input_prev_steps {
                    drop(el);
                }
                // Ok(field_result.hits)
                Ok(())
            }
            PlanStepType::ValueIdToParent {
                channels,
                path,
                trace_info: joop,
                ..
            } => {
                channels.output_sending_to_next_steps.send(join_to_parent_with_score(persistence, &channels.input_prev_steps[0].recv()?, &path, &joop)?)?;
                for el in channels.input_prev_steps {
                    drop(el);
                }
                drop(channels.output_sending_to_next_steps);
                Ok(())
            }
            PlanStepType::Boost {
                req,
                channels,
                ..
            } => {
                let mut input = channels.input_prev_steps[0].recv()?;
                add_boost(persistence, &req, &mut input)?; //TODO Wrap
                channels.output_sending_to_next_steps.send(input)?;
                for el in channels.input_prev_steps {
                    drop(el);
                }
                drop(channels.output_sending_to_next_steps);
                Ok(())
            }
            PlanStepType::Union {
                steps,
                channels,
                ..
            } => {
                debug_time!("union total");
                // execute_steps(steps, persistence)?;
                debug_time!("union netto");
                channels.output_sending_to_next_steps.send(union_hits_vec(get_data(channels.input_prev_steps)?))?;
                drop(channels.output_sending_to_next_steps);
                Ok(())
            }
            PlanStepType::Intersect {
                steps,
                channels,
                ..
            } => {
                debug_time!("intersect total");
                // execute_steps(steps, persistence)?;
                debug_time!("intersect netto");
                channels.output_sending_to_next_steps.send(intersect_hits_vec(get_data(channels.input_prev_steps)?))?;
                drop(channels.output_sending_to_next_steps);
                Ok(())
            }
            PlanStepType::FromAttribute { steps, .. } => {
                // execute_steps(steps, persistence)?;
                // output_sending_to_next_steps.send(intersect_hits(input_prev_steps.iter().map(|el| el.recv().unwrap()).collect()));
                // drop(output_sending_to_next_steps);
                Ok(())
            }
        }
    }
}
// use fnv::FnvHashSet;
// fn get_all_field_request_parts(request: &Request) -> FnvHashSet<RequestSearchPart> {
//     if let Some(or) = request.or {
//         return or.iter().map(|el|{
//             get_all_field_request_parts(el)
//         }).map(|map|map.iter()).collect();
//     }
//     FnvHashSet::default()
// }

#[cfg_attr(feature = "flame_it", flame)]
pub fn plan_creator(request: Request, plan: &mut Plan<PlanStepType>) -> (PlanStepType, PlanDataReceiver) {
    let (tx, rx): (PlanDataSender, PlanDataReceiver) = unbounded();

    if let Some(or) = request.or {
        let steps: Vec<PlanStepType> = or.iter().map(|x| plan_creator(x.clone(), plan).0).collect();
        let result_channels_from_prev_steps = steps.iter().map(|el| el.get_output()).collect();
        let step = (PlanStepType::Union {
                steps: steps.clone(),
                channels: PlanStepDataChannels {
                    input_prev_steps: result_channels_from_prev_steps,
                    output_sending_to_next_steps: tx,
                    plans_output_receiver_for_next_step: rx.clone(),
                }
            }, rx);

        plan.add_step(step.0.clone());
        plan.steps.extend(steps.clone());
        for dub_step in steps {
            plan.add_dependency(&step.0, &dub_step);
        }

        step
    } else if let Some(ands) = request.and {
        let steps: Vec<PlanStepType> = ands.iter().map(|x| plan_creator(x.clone(), plan).0).collect();
        let result_channels_from_prev_steps = steps.iter().map(|el| el.get_output()).collect();
        let step =(PlanStepType::Intersect {
                steps: steps.clone(),
                channels: PlanStepDataChannels {
                    input_prev_steps: result_channels_from_prev_steps,
                    output_sending_to_next_steps: tx,
                    plans_output_receiver_for_next_step: rx.clone(),
                }
                // input_prev_steps: result_channels_from_prev_steps,
                // output_sending_to_next_steps: tx,
                // plans_output_receiver_for_next_step: rx,
            }, rx);

        plan.add_step(step.0.clone());
        plan.steps.extend(steps.clone());
        for dub_step in steps {
            plan.add_dependency(&step.0, &dub_step);
        }

        step
    } else if let Some(part) = request.search.clone() {
        // TODO Tokenize query according to field
        // part.terms = part.terms.iter().map(|el| util::normalize_text(el)).collect::<Vec<_>>();
        plan_creator_search_part(part, request, plan)
    } else {
        //TODO HANDLE SUGGEST
        //TODO ADD ERROR
        // plan_creator_search_part(request.search.as_ref().unwrap().clone(), request)
        panic!("missing 'and' 'or' 'search' in request - suggest not yet handled in search api {:?}", request);
    }
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn plan_creator_search_part(request_part: RequestSearchPart, mut request: Request, plan: &mut Plan<PlanStepType>) -> (PlanStepType, PlanDataReceiver) {
    let paths = util::get_steps_to_anchor(&request_part.path);

    let (field_tx, field_rx): (PlanDataSender, PlanDataReceiver) = unbounded();

    let fast_field = request.boost.is_none() && !request_part.snippet.unwrap_or(false); // fast_field disabled for boosting or _highlighting_ currently

    let store_term_id_hits = request.why_found || request.text_locality;

    let plan_request_part = PlanRequestSearchPart{request:request_part, store_term_id_hits, store_term_texts: request.why_found, fast_field, ..Default::default()};

    if fast_field {
        let step = (PlanStepType::FieldSearchAndAnchorResolve {
            req: plan_request_part,
            channels: PlanStepDataChannels{
                input_prev_steps: vec![],
                output_sending_to_next_steps: field_tx,
                plans_output_receiver_for_next_step: field_rx.clone(),
            }
        }, field_rx);
        plan.add_step(step.0.clone());
        step
    } else {
        let mut steps = vec![];
        //search in fields
        steps.push(PlanStepType::FieldSearchAndAnchorResolve {
            req: plan_request_part,
            channels: PlanStepDataChannels{
                input_prev_steps: vec![],
                output_sending_to_next_steps: field_tx,
                plans_output_receiver_for_next_step: field_rx.clone(),
            }
        });

        let (mut tx, mut rx): (PlanDataSender, PlanDataReceiver) = unbounded();

        steps.push(PlanStepType::ValueIdToParent {
            path: paths.last().unwrap().add(VALUE_ID_TO_PARENT),
            trace_info: "term hits hit to column".to_string(),
            channels: PlanStepDataChannels{
                input_prev_steps: vec![field_rx.clone()],
                output_sending_to_next_steps: tx.clone(),
                plans_output_receiver_for_next_step: rx.clone(),
            }
        });

        for i in (0..paths.len() - 1).rev() {
            if request.boost.is_some() {
                request.boost.as_mut().unwrap().retain(|boost| {
                    let apply_boost = boost.path.starts_with(&paths[i]);
                    if apply_boost {
                        let (next_tx, next_rx): (PlanDataSender, PlanDataReceiver) = unbounded();
                        tx = next_tx;
                        steps.push(PlanStepType::Boost {
                            // plans_output_receiver_for_next_step: next_rx.clone(),
                            req: boost.clone(),
                            // input_prev_steps: vec![rx.clone()],
                            // output_sending_to_next_steps: tx.clone(),
                            channels: PlanStepDataChannels{
                                input_prev_steps: vec![rx.clone()],
                                output_sending_to_next_steps: tx.clone(),
                                plans_output_receiver_for_next_step: next_rx.clone(),
                            }
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
                path: paths[i].add(VALUE_ID_TO_PARENT),
                trace_info: "Joining to anchor".to_string(),
                channels: PlanStepDataChannels{
                    input_prev_steps: vec![rx.clone()],
                    output_sending_to_next_steps: tx.clone(),
                    plans_output_receiver_for_next_step: next_rx.clone(),
                }
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
                    req: boost.clone(),
                    channels: PlanStepDataChannels{
                        input_prev_steps: vec![rx.clone()],
                        output_sending_to_next_steps: tx.clone(),
                        plans_output_receiver_for_next_step: next_rx.clone(),
                    }
                });
                debug!("PlanCreator Step {:?}", boost);
                rx = next_rx;
            }
        }

        let step = (PlanStepType::FromAttribute {
            steps: steps.clone(),
            channels: PlanStepDataChannels{ // unused currently
                input_prev_steps: vec![],
                output_sending_to_next_steps: tx.clone(),
                plans_output_receiver_for_next_step: rx.clone(),
            }
        }, rx);

        // steps.push(step.0.clone());
        plan.steps.extend(steps);
        step
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

use crossbeam;
#[cfg_attr(feature = "flame_it", flame)]
pub fn execute_step_in_parrael(steps: Vec<PlanStepType>, persistence: &Persistence) -> Result<(), SearchError> {

    crossbeam::scope(|scope| {
        for step in steps {
            scope.spawn(move || {
                let res = step.execute_step(persistence);
                if res.is_err(){
                    panic!("{:?}", res.unwrap_err());
                }
            });
        }
    });

    Ok(())
}
