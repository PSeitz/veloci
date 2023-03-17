use crate::plan_creator::{channel::*, *};
use std::io::Write;

#[derive(Debug)]
/// Plan creates a plan based on a search::Request
#[derive(Default)]
pub struct Plan {
    pub steps: Vec<Box<dyn PlanStepTrait>>,
    pub dependencies: Vec<Dependency>,
    pub plan_result: Option<PlanDataReceiver>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Dependency {
    step_index: usize,
    depends_on: usize,
}



impl Plan {
    pub(crate) fn add_dependency(&mut self, step_index: usize, depends_on: usize) {
        self.dependencies.push(Dependency { step_index, depends_on });
    }

    /// return the position in the array, which can be used as an id
    pub(crate) fn add_step(&mut self, step: Box<dyn PlanStepTrait>) -> usize {
        self.steps.push(step);
        self.steps.len() - 1
    }

    /// return the position in the array, which can be used as an id
    pub(crate) fn get_step_channel(&mut self, step_id: usize) -> &mut PlanStepDataChannels {
        self.steps[step_id].get_channel()
    }

    // fn get_dependencies(&self, step_index: usize) -> Vec<Dependency> {
    //     self.dependencies.iter().filter(|dep|dep.step_index == step_index).cloned().collect()
    // }

    pub fn get_ordered_steps(self) -> Vec<Vec<Box<dyn PlanStepTrait>>> {
        let mut ordered_steps = vec![];
        let mut remaining_steps: Vec<_> = self.steps.into_iter().enumerate().collect();
        let dep = self.dependencies;
        while !remaining_steps.is_empty() {
            let current_remaining_step_ids: Vec<_> = remaining_steps.iter().map(|el| el.0).collect();
            let steps_with_fullfilled_dependencies: Vec<_> = remaining_steps
                .drain_filter(|step_with_index| {
                    // let steps_dependencies = self.get_dependencies(step_with_index.0);
                    let steps_dependencies: Vec<Dependency> = dep.iter().filter(|dep| dep.step_index == step_with_index.0).cloned().collect();
                    let unfulfilled_dependencies: Vec<_> = steps_dependencies
                        .iter()
                        .filter(|dep| {
                            current_remaining_step_ids.iter().any(|step_id| *step_id == dep.depends_on) // check if depends_on is in current_remaining_step_ids
                        })
                        .collect();

                    unfulfilled_dependencies.is_empty()
                })
                .collect();

            if steps_with_fullfilled_dependencies.is_empty() {
                panic!("invalid plan created");
            }
            // ordered_steps.push(steps_with_fullfilled_dependencies.iter().map(|step_with_index|*step_with_index.1.clone()).collect());
            let vecco: Vec<_> = steps_with_fullfilled_dependencies.into_iter().map(|step_with_index| step_with_index.1).collect();
            ordered_steps.push(vecco);
        }
        ordered_steps
    }
}

type Nd = (usize, String);
type Ed<'a> = &'a Dependency;
struct Graph<'c> {
    nodes: &'c Vec<Box<dyn PlanStepTrait>>,
    edges: Vec<Dependency>,
}

pub fn render_plan_to<W: Write>(plan: &Plan, output: &mut W) {
    let graph = Graph {
        nodes: &plan.steps,
        edges: plan.dependencies.to_vec(),
    };

    dot::render(&graph, output).unwrap()
}

impl<'a, 'c> dot::Labeller<'a, Nd, Ed<'a>> for Graph<'c> {
    fn graph_id(&'a self) -> dot::Id<'a> {
        dot::Id::new("example2").unwrap()
    }

    fn node_id(&'a self, n: &Nd) -> dot::Id<'a> {
        dot::Id::new(format!("N{}", n.0)).unwrap()
    }

    fn node_label<'b>(&'b self, n: &Nd) -> dot::LabelText<'b> {
        dot::LabelText::LabelStr(n.1.to_string().into())
    }

    fn edge_label<'b>(&'b self, _: &Ed<'_>) -> dot::LabelText<'b> {
        dot::LabelText::LabelStr("".into())
    }
}

impl<'a, 'c> dot::GraphWalk<'a, Nd, Ed<'a>> for Graph<'c> {
    // fn nodes(&self) -> dot::Nodes<'a,Nd> { (0..self.nodes.len()).collect() }
    fn nodes(&self) -> dot::Nodes<'a, Nd> {
        self.nodes.iter().enumerate().map(|(i, el)| (i, format!("{}", el))).collect()
    }

    fn edges(&'a self) -> dot::Edges<'a, Ed<'a>> {
        self.edges.iter().collect()
    }

    fn source(&self, e: &Ed<'_>) -> Nd {
        (e.depends_on, format!("{}", self.nodes[e.depends_on]))
    }

    fn target(&self, e: &Ed<'_>) -> Nd {
        (e.step_index, format!("{}", self.nodes[e.step_index]))
    }
}
