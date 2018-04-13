extern crate env_logger;
extern crate flexi_logger;
extern crate fst;
extern crate fst_levenshtein;
extern crate half;
extern crate itertools;
extern crate search_lib;
#[macro_use]
extern crate serde_json;

extern crate fnv;
extern crate serde;

#[macro_use]
extern crate log;
#[macro_use]
extern crate measure_time;

// use search_lib::doc_loader::*;
// use search_lib::search;
// use search_lib::query_generator;
// use search_lib::search_field;
// use search_lib::persistence::Persistence;
use search_lib::persistence;

use fnv::FnvHashMap;
use itertools::Itertools;

#[allow(unused_imports)]
use fst::{IntoStreamer, MapBuilder, Set};
// use fst_levenshtein::Levenshtein;
use std::io::prelude::*;
use half::f16;

fn main() {
    // env_logger::init().unwrap();
    search_lib::trace::enable_log();
    let persistence = persistence::Persistence::load("thalia".to_string()).unwrap();

    let paths = vec![
        "ISMMEDIATYPE.textindex",
        "ISMSUBTITLE1.textindex",
        "ZZONLINE_KAT.textindex",
        "LVORM.textindex",
        "REIHE[].textindex",
        "ISMPUBLDATE.textindex",
        "MAKTX[].textindex",
        "ZZSCHULBUCH_NR.textindex",
        "VERLAG[].textindex",
        "GENRE.textindex",
        "ISMLANGUAGES.textindex",
        "ZZWERKID.textindex",
        "MATKL.textindex",
        "ISMLANGU_DESCR.textindex",
        "AUTOR[].textindex",
        "ISMARTIST.textindex",
        "ISMMEDIA_DESCR.textindex",
        "ZZISMN.textindex",
        "MEINS.textindex",
        "ISMSUBTITLE3.textindex",
        "ZZSW2_KEY.textindex",
        "HIERARCHIE.textindex",
        "ZZSCHLAG2.textindex",
        "ZZHERSTELLER_NR.textindex",
        "ISMTITLE.textindex",
        "EAN11.textindex",
        "ISMSUBTITLE2.textindex",
        "ISMORIDCODE.textindex",
        "MATNR.textindex",
        "ZZNACHFOLGE.textindex",
        "MTART.textindex",
        "GTEXT[].textindex",
        "ZZMYINFOARTNR.textindex",
        "SCHLAGWORT[].textindex",
    ];

    // let paths = vec!["ISMMEDIA_DESCR.textindex",];
    // let paths = vec!["ISMMEDIATYPE.textindex"];

    let mut super_total_uncomp = 0;
    let mut super_total_comp = 0;
    let mut super_total_comp_enc_score = 0;
    let mut super_total_comp_enc_score_three = 0;

    for path in paths {
        let token_to_anchor_score = persistence.get_token_to_anchor(&path).unwrap();

        let mut total_uncomp = 0;
        let mut total_comp = 0;
        let mut total_comp_enc_score = 0;
        let mut total_comp_enc_score_three = 0;

        for id in 0..token_to_anchor_score.get_max_id() {
            if let Some(text_id_score) = token_to_anchor_score.get_scores(id as u32) {
                // let increases:Vec<_> = [0 as u32].into_iter().cloned().chain(text_id_score.iter().map(|el|el.id as u32))
                //     .tuples().map(|(id1, id2)|id2 as i32 - id1 as i32 - 1).collect();
                // let sum:i32 = increases.iter().sum();

                let mut increases = vec![];
                let mut last = 0;
                for el in text_id_score.iter() {
                    increases.push(el.id - last);
                    last = el.id;
                }

                // let scores:Vec<_> = text_id_score.iter().map(|el|el.score.to_f32() as u32).collect();

                let scores_bytes = text_id_score.len(); // 1 byte for each element
                total_comp_enc_score += scores_bytes;
                total_comp_enc_score_three += scores_bytes;
                total_comp += scores_bytes;
                total_uncomp += text_id_score.len() * 4;

                info!("{:?}", &increases[0..(std::cmp::min(1000, increases.len()) as usize)]);

                let mut map_scores = text_id_score.iter().map(|el| el.score.to_f32() as u32).fold(FnvHashMap::default(), |mut m, c| {
                    *m.entry(c).or_insert(0) += 1;
                    m
                });

                total_comp += increases
                    .iter()
                    .map(|&inc| {
                        if inc < 128 {
                            1
                        } else if inc < 16000 {
                            2
                        } else if inc < 2_000_000 {
                            3
                        } else {
                            4
                        }
                    })
                    .sum::<usize>();

                // println!("total_comp {:?}", total_comp);
                // println!("increases {:?}", increases.len());

                let most_occurences = map_scores.values().max().unwrap();

                // let occurences = num_occurences_of_scores[0];

                let sum_comp_enc_score = increases
                    .iter()
                    .map(|&inc| {
                        if inc < 64 {
                            1
                        } else if inc < 8000 {
                            2
                        } else if inc < 1_000_000 {
                            3
                        } else {
                            4
                        }
                    })
                    .sum::<usize>();
                total_comp_enc_score += sum_comp_enc_score;
                total_comp_enc_score -= most_occurences;

                if map_scores.len() >= 3 {
                    let mut num_occurences_of_scores: Vec<_> = map_scores.values().collect();
                    num_occurences_of_scores.sort();
                    num_occurences_of_scores = num_occurences_of_scores.into_iter().rev().collect();
                    let occurences = num_occurences_of_scores[0] + num_occurences_of_scores[1] + num_occurences_of_scores[2];

                    total_comp_enc_score_three += increases
                        .iter()
                        .map(|&inc| {
                            if inc < 32 {
                                1
                            } else if inc < 4000 {
                                2
                            } else if inc < 500_000 {
                                3
                            } else {
                                4
                            }
                        })
                        .sum::<usize>();
                    total_comp_enc_score_three -= occurences;
                } else {
                    //not enough values, take single encoded
                    total_comp_enc_score_three += sum_comp_enc_score;
                    total_comp_enc_score_three -= most_occurences;
                }

                let maybe_header_size = 4;
                total_comp_enc_score += maybe_header_size + maybe_header_size; //DOUBLE HEADERSIZE FOR ENCODING ??
                total_comp_enc_score_three += maybe_header_size + maybe_header_size; //DOUBLE HEADERSIZE FOR ENCODING ??
                total_comp += maybe_header_size + maybe_header_size; //DOUBLE HEADERSIZE FOR ENCODING ??
                total_uncomp += maybe_header_size;
            }
        }

        println!("path                  {:?}", path);
        if total_uncomp > 2_000_000 {
            println!("total_uncomp          {:?}MB", total_uncomp / 1_000_000);
            println!("total_comp            {:?}MB", total_comp / 1_000_000);
            println!("total_comp_enc_score  {:?}MB", total_comp_enc_score / 1_000_000);
            println!("total_comp_enc_score_three  {:?}MB", total_comp_enc_score_three / 1_000_000);
        } else if total_uncomp > 2_000 {
            println!("total_uncomp          {:?}KB", total_uncomp / 1_000);
            println!("total_comp            {:?}KB", total_comp / 1_000);
            println!("total_comp_enc_score  {:?}KB", total_comp_enc_score / 1_000);
            println!("total_comp_enc_score_three  {:?}KB", total_comp_enc_score_three / 1_000);
        } else {
            println!("total_uncomp          {:?}", total_uncomp);
            println!("total_comp            {:?}", total_comp);
            println!("total_comp_enc_score  {:?}", total_comp_enc_score);
            println!("total_comp_enc_score_three  {:?}", total_comp_enc_score_three);
        }
        super_total_uncomp += total_uncomp;
        super_total_comp += total_comp;
        super_total_comp_enc_score += total_comp_enc_score;
        super_total_comp_enc_score_three += total_comp_enc_score_three;
        // println!("total_uncomp          {:?}", total_uncomp);
        // println!("total_comp            {:?}", total_comp);
        // println!("total_comp_enc_score  {:?}", total_comp_enc_score);
    }

    println!("ganz_total_uncomp                {:?}MB", super_total_uncomp / 1_000_000);
    println!("ganz_total_comp                  {:?}MB", super_total_comp / 1_000_000);
    println!("ganz_total_comp_enc_score        {:?}MB", super_total_comp_enc_score / 1_000_000);
    println!("ganz_total_comp_enc_score_three  {:?}MB", super_total_comp_enc_score_three / 1_000_000);
}
