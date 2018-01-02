use util;
use util::concat;
use persistence::Persistence;
use search::*;
use search::RequestSearchPart;
use search::RequestBoostPart;
#[allow(unused_imports)]
use search::Request;
#[allow(unused_imports)]
use search::union_hits;
#[allow(unused_imports)]
use search::intersect_hits;
use search::SearchError;
use search_field;
use search::add_boost;
#[allow(unused_imports)]
use fnv::FnvHashMap;
#[allow(unused_imports)]
use trie::map;

use crossbeam_channel::unbounded;
use crossbeam_channel;
use search_field::*;


type PlanDataSender = crossbeam_channel::Sender<SearchFieldResult>;
type PlanDataReceiver = crossbeam_channel::Receiver<SearchFieldResult>;

#[derive(Clone, Debug)]
pub enum PlanStepType {
    FieldSearch{req:RequestSearchPart, input_prev_steps:Vec<PlanDataReceiver>, output_next_steps:PlanDataSender, plans_output:PlanDataReceiver},
    ValueIdToParent{path:String, trace_info:String, input_prev_steps:Vec<PlanDataReceiver>, output_next_steps:PlanDataSender, plans_output:PlanDataReceiver},
    Boost{req:RequestBoostPart, input_prev_steps:Vec<PlanDataReceiver>, output_next_steps:PlanDataSender, plans_output:PlanDataReceiver},
    Union{steps:Vec<PlanStepType>, input_prev_steps:Vec<PlanDataReceiver>, output_next_steps:PlanDataSender, plans_output:PlanDataReceiver},
    Intersect{steps:Vec<PlanStepType>, input_prev_steps:Vec<PlanDataReceiver>, output_next_steps:PlanDataSender, plans_output:PlanDataReceiver},
    FromAttribute{steps:Vec<PlanStepType>, output_next_steps:PlanDataReceiver, plans_output:PlanDataReceiver},
}

pub trait OutputProvider {
    fn get_output(&self) -> PlanDataReceiver;
}

impl OutputProvider for PlanStepType {
    fn get_output(&self) -> PlanDataReceiver{
        match self {
            &PlanStepType::FieldSearch{ref plans_output, ..} => {
                plans_output.clone()
            }
            &PlanStepType::ValueIdToParent{ref plans_output, ..} => {
                plans_output.clone()
            }
            &PlanStepType::Boost{ref plans_output, ..} => {
                plans_output.clone()
            }
            &PlanStepType::Union{ref plans_output, ..} => {
                plans_output.clone()
            }
            &PlanStepType::Intersect{ref plans_output, ..} => {
                plans_output.clone()
            }
            &PlanStepType::FromAttribute{ref plans_output, ..} => {
                plans_output.clone()
            }
        }
    }
}


fn get_data(input_prev_steps:Vec<PlanDataReceiver>) -> Result<Vec<SearchFieldResult>, SearchError> {
    let mut dat = vec![];
    for el in input_prev_steps {
        dat.push(el.recv()?);
    }
    Ok(dat)
}

pub trait StepExecutor {
    fn execute_step(self, persistence: &Persistence) -> Result<(), SearchError>;
}
impl StepExecutor for PlanStepType {

    #[allow(unused_variables)]
    #[flame]
    fn execute_step(self, persistence: &Persistence) -> Result<(), SearchError>{
        match self {
            PlanStepType::FieldSearch{mut req, input_prev_steps, output_next_steps, ..} => {
                let field_result = search_field::get_hits_in_field(persistence, &mut req)?;
                output_next_steps.send(field_result)?;
                // Ok(field_result.hits)
                Ok(())
            }
            PlanStepType::ValueIdToParent{input_prev_steps, output_next_steps, path, trace_info:joop, ..} => {
                output_next_steps.send(join_to_parent_with_score(persistence, input_prev_steps[0].recv()?, &path, &joop)?)?;
                Ok(())
            }
            PlanStepType::Boost{req,input_prev_steps, output_next_steps, ..} => {
                let mut input = input_prev_steps[0].recv()?;
                add_boost(persistence, &req, &mut input)?;
                output_next_steps.send(input)?;
                Ok(())
            }
            PlanStepType::Union{steps, input_prev_steps, output_next_steps, ..} => {
                execute_steps(steps, persistence)?;
                output_next_steps.send(union_hits(get_data(input_prev_steps)?))?;
                Ok(())
            }
            PlanStepType::Intersect{steps, input_prev_steps, output_next_steps, ..} => {
                execute_steps(steps, persistence)?;
                output_next_steps.send(intersect_hits(get_data(input_prev_steps)?))?;
                Ok(())
            }
            PlanStepType::FromAttribute{steps,  ..} => {
                execute_steps(steps, persistence)?;
                // output_next_steps.send(intersect_hits(input_prev_steps.iter().map(|el| el.recv().unwrap()).collect()));
                Ok(())
            }
        }
    }
}



#[flame]
pub fn plan_creator(request: Request) -> PlanStepType {

    let (tx, rx):(PlanDataSender, PlanDataReceiver) = unbounded();

    if let Some(or) = request.or {
        let steps:Vec<PlanStepType> = or.iter().map(|x| {plan_creator(x.clone()) }).collect();
        let results_from_prev_steps = steps.iter().map(|el| el.get_output()).collect();
        PlanStepType::Union{steps, input_prev_steps:results_from_prev_steps, output_next_steps:tx, plans_output:rx}

    } else if let Some(ands) = request.and {
        let steps:Vec<PlanStepType> = ands.iter().map(|x| {plan_creator(x.clone()) }).collect();
        let results_from_prev_steps = steps.iter().map(|el| el.get_output()).collect();
        PlanStepType::Intersect{steps, input_prev_steps:results_from_prev_steps, output_next_steps:tx, plans_output:rx}

    } else if let Some(mut part) = request.search {
        // TODO Tokenize query according to field
        // part.terms = part.terms.iter().map(|el| util::normalize_text(el)).collect::<Vec<_>>();
        plan_creator_search_part(part, request.boost)
    } else {
        //TODO ADD ERROR
        plan_creator_search_part(request.search.unwrap(), request.boost)
    }

}


#[flame]
pub fn plan_creator_search_part(request: RequestSearchPart, mut boost: Option<Vec<RequestBoostPart>>) -> PlanStepType {
    let paths = util::get_steps_to_anchor(&request.path);

    let mut steps = vec![];

    let (field_tx, field_rx):(PlanDataSender, PlanDataReceiver) = unbounded();

    //search in fields
    steps.push(PlanStepType::FieldSearch{plans_output: field_rx.clone() ,req:request, input_prev_steps: vec![], output_next_steps: field_tx});

    let (mut tx, mut rx):(PlanDataSender, PlanDataReceiver) = unbounded();

    steps.push(PlanStepType::ValueIdToParent{plans_output: rx.clone() ,input_prev_steps: vec![field_rx], output_next_steps: tx.clone(), path: concat(&paths.last().unwrap(), ".valueIdToParent"), trace_info: "term hits hit to column".to_string()});

    for i in (0..paths.len() - 1).rev() {

        if boost.is_some() {
            boost.as_mut().unwrap().retain(|boost| {
                let apply_boost = boost.path.starts_with(&paths[i]);
                if apply_boost {
                    let (next_tx, next_rx):(PlanDataSender, PlanDataReceiver) = unbounded();
                    tx = next_tx;
                    steps.push(PlanStepType::Boost{plans_output: next_rx.clone() ,req:boost.clone(), input_prev_steps: vec![rx.clone()], output_next_steps: tx.clone()});
                    rx = next_rx;
                }
                apply_boost
            });
        }

        let (next_tx, next_rx):(PlanDataSender, PlanDataReceiver) = unbounded();
        tx = next_tx;
        // let will_apply_boost = boost.map(|boost| boost.path.starts_with(&paths[i])).unwrap_or(false);
        steps.push(PlanStepType::ValueIdToParent{plans_output: next_rx.clone() ,input_prev_steps: vec![rx.clone()], output_next_steps: tx.clone(), path: concat(&paths[i], ".valueIdToParent"), trace_info: "Joining to anchor".to_string()});

        rx = next_rx;
    }

    PlanStepType::FromAttribute{plans_output:rx.clone() ,steps:steps, output_next_steps:rx}

    // (steps, rx)

}

// #[flame]
// pub fn execute_step(step: PlanStepType, persistence: &Persistence) -> Result<(), SearchError>
// {

//     match step {
//         PlanStepType::FieldSearch{mut req, input_prev_steps, output_next_steps, ..} => {
//             let field_result = search_field::get_hits_in_field(persistence, &mut req)?;
//             output_next_steps.send(field_result)?;
//             // Ok(field_result.hits)
//             Ok(())
//         }
//         PlanStepType::ValueIdToParent{input_prev_steps, output_next_steps, path, trace_info:joop, ..} => {
//             output_next_steps.send(join_to_parent_with_score(persistence, input_prev_steps[0].recv().unwrap(), &path, &joop)?)?;
//             Ok(())
//         }
//         PlanStepType::Boost{req,input_prev_steps, output_next_steps, ..} => {
//             let mut input = input_prev_steps[0].recv().unwrap();
//             add_boost(persistence, &req, &mut input)?;
//             output_next_steps.send(input)?;
//             Ok(())
//         }
//         PlanStepType::Union{steps, input_prev_steps, output_next_steps, ..} => {
//             execute_steps(steps, persistence)?;
//             output_next_steps.send(union_hits(input_prev_steps.iter().map(|el| el.recv().unwrap()).collect()))?;
//             Ok(())
//         }
//         PlanStepType::Intersect{steps, input_prev_steps, output_next_steps, ..} => {
//             execute_steps(steps, persistence)?;
//             output_next_steps.send(intersect_hits(input_prev_steps.iter().map(|el| el.recv().unwrap()).collect()))?;
//             Ok(())
//         }
//         PlanStepType::FromAttribute{steps, output_next_steps, ..} => {
//             execute_steps(steps, persistence)?;
//             // output_next_steps.send(intersect_hits(input_prev_steps.iter().map(|el| el.recv().unwrap()).collect()));
//             Ok(())
//         }
//     }
// }
use rayon::prelude::*;


#[flame]
pub fn execute_steps(steps: Vec<PlanStepType>, persistence: &Persistence) -> Result<(), SearchError>
{

    let r: Result<Vec<_>, SearchError> = steps.into_par_iter().map(|step|{
        step.execute_step(persistence)
        // execute_step(step.clone(), persistence)
    }).collect();

    if r.is_err(){
        Err(r.unwrap_err())
    }else {
        Ok(())
    }

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



