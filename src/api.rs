

#[derive(Debug)]
struct SearchRequest {
    fields: Vec<String>,
    query: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub top: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub skip: Option<usize>,

    #[serde(default)]
    pub explain: bool,

}

// {
//     query: "die suchterme",
//     fields : ["*"],
//     top: 10,
//     skip: 0
// }