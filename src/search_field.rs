

use std::io::prelude::*;
use std::io::SeekFrom;
use str;
use persistence;
use persistence::Persistence;
use search::RequestSearchPart;
use search::SearchError;
use fnv::FnvHashMap;
use util::concat;
use util;
use std::cmp;

fn get_default_score(term1: &str, term2: &str) -> f32{
    return 2.0/(distance(term1, term2) as f32 + 0.2 )
}
fn get_default_score2(distance: u32) -> f32{
    return 2.0/(distance as f32 + 0.2 )
}

#[inline(always)]
fn get_text_lines<F>(persistence:&Persistence, path: &str, exact_search:Option<String>, character: Option<&str>, mut fun: F) -> Result<(), SearchError>
where F: FnMut(&str, u32) {

    if exact_search.is_some() {
        let mut faccess:persistence::FileSearch = persistence.get_file_search(path);
        let result = faccess.binary_search(&exact_search.unwrap(), persistence)?;
        if result.1 != -1 {
            fun(&result.0, result.1 as u32 );
        }

    }else if character.is_some() {
        debug!("Search CharOffset for: {:?}", character.unwrap());
        let char_offset_info_opt = persistence.get_create_char_offset_info(path, character.unwrap())?;
        debug!("CharOffset: {:?}", char_offset_info_opt);
        if char_offset_info_opt.is_none() {
            return Ok(())
        }
        let char_offset_info = char_offset_info_opt.unwrap();
        let mut f = persistence.get_file_handle(path)?;
        let mut buffer:Vec<u8> = Vec::with_capacity((char_offset_info.byte_range_end - char_offset_info.byte_range_start) as usize);
        unsafe { buffer.set_len(char_offset_info.byte_range_end as usize - char_offset_info.byte_range_start as usize); }

        f.seek(SeekFrom::Start(char_offset_info.byte_range_start as u64))?;
        f.read_exact(&mut buffer)?;
        // let s = unsafe {str::from_utf8_unchecked(&buffer)};
        let s = str::from_utf8(&buffer)?; // @Temporary  -> use unchecked if stable
        // trace!("Loaded Text: {}", s);
        let lines = s.lines();
        let mut pos = 0;
        for line in lines{
            fun(&line, char_offset_info.line_offset as u32 + pos );
            pos += 1;
        }
        debug!("Checked {:?} lines", pos);

    }else{
        let mut f = persistence.get_file_handle(path)?;
        let mut s = String::new();
        f.read_to_string(&mut s)?;
        let lines = s.lines();
        for (line_pos, line) in lines.enumerate(){
            fun(&line, line_pos as u32)
        }
    }
    Ok(())
}


pub fn get_hits_in_field(persistence:&Persistence, mut options: &mut RequestSearchPart, term: &str) -> Result<FnvHashMap<u32, f32>, SearchError> {
    debugTime!("get_hits_in_field");
    let mut hits:FnvHashMap<u32, f32> = FnvHashMap::default();
    // let checks:Vec<Fn(&str) -> bool> = Vec::new();
    let term_chars = term.chars().collect::<Vec<char>>();
    // options.first_char_exact_match = options.exact || options.levenshtein_distance == 0 || options.starts_with.is_some(); // TODO fix

    if options.levenshtein_distance.unwrap_or(0) == 0 {
        options.exact = Some(true);
    }

    let start_char = if options.exact.unwrap_or(false) || options.levenshtein_distance.unwrap_or(0) == 0 || options.starts_with.is_some() && term_chars.len() >= 2 {
        Some(term_chars[0].to_string() + &term_chars[1].to_string())
    }
    else if options.first_char_exact_match.unwrap_or(false) { Some(term_chars[0].to_string() )
    }
    else { None };

    let value = start_char.as_ref().map(String::as_ref);

    trace!("Will Check distance {:?}", options.levenshtein_distance.unwrap_or(0) != 0);
    trace!("Will Check exact {:?}", options.exact);
    trace!("Will Check starts_with {:?}", options.starts_with);
    {
        let teh_callback = |line: &str, line_pos: u32| {
            // trace!("Checking {} with {}", line, term);
            let distance = if options.levenshtein_distance.unwrap_or(0) != 0 { Some(distance(term, line))} else { None };
            if (options.exact.unwrap_or(false) &&  line == term)
                || (distance.is_some() && distance.unwrap() <= options.levenshtein_distance.unwrap_or(0))
                || (options.starts_with.is_some() && line.starts_with(options.starts_with.as_ref().unwrap())  )
                // || (options.customCompare.is_some() && options.customCompare.unwrap(line, term))
                {
                // let score = get_default_score(term, line);
                let score = if distance.is_some() {get_default_score2(distance.unwrap())} else {get_default_score(term, line)};
                debug!("Hit: {:?}\tid: {:?} score: {:?}", line, line_pos, score);
                hits.insert(line_pos, score);
            }
        };
        let exact_search = if options.exact.unwrap_or(false) {Some(term.to_string())} else {None};
        get_text_lines(persistence, &options.path, exact_search, value, teh_callback)?;
    }
    debug!("{:?} hits in textindex {:?}", hits.len(), &options.path);
    trace!("hits in textindex: {:?}", hits);
    add_token_results(persistence, &options.path, &mut hits);
    Ok(hits)

}


pub fn add_token_results(persistence:&Persistence, path:&str, hits: &mut FnvHashMap<u32, f32>){
    debugTime!("add_token_results");

    let has_tokens = persistence.meta_data.fulltext_indices.get(path).map_or(false, |fulltext_info| fulltext_info.tokenize);
    debug!("has_tokens {:?} {:?}", path, has_tokens);
    if !has_tokens { return; }
    // var hrstart = process.hrtime()
    // let cache_lock = persistence::INDEX_64_CACHE.read().unwrap();
    let text_offsets = persistence.index_64.get(&concat(&path, ".offsets"))
        .expect(&format!("Could not find {:?} in index_64 cache", concat(&path, ".offsets")));

    let key = (concat(&path, ".textindex.tokens.tokenValIds"), concat(&path, ".textindex.tokens.parentValId"));
    let token_kvdata = persistence.index_id_to_parent.get(&key).expect(&format!("Could not find {:?} in index_id_to_parent cache", key));
    let mut token_hits:FnvHashMap<u32, f32> = FnvHashMap::default();
    for (value_id, _) in hits.iter() {
        // let parent_ids_for_token = token_kvdata.get_parent_val_ids(*value_id, &cache_lock);

        let ref parent_ids_for_token_opt = token_kvdata.get(*value_id as usize);
        parent_ids_for_token_opt.map(|parent_ids_for_token|{
            if parent_ids_for_token.len() > 0 {
                token_hits.reserve(parent_ids_for_token.len());
                for token_parentval_id in parent_ids_for_token {
                    let parent_text_length = text_offsets[1 + *token_parentval_id as usize] - text_offsets[*token_parentval_id as usize];
                    let token_text_length  = text_offsets[1 + *value_id as usize] - text_offsets[*value_id as usize];
                    let adjusted_score = 2.0/(parent_text_length as f32 - token_text_length as f32) + 0.2;
                    // if (adjusted_score < 0) throw new Error('asdf')

                    let the_score = token_hits.entry(*token_parentval_id as u32) // @Temporary
                        .or_insert(*hits.get(token_parentval_id).unwrap_or(&0.0));
                    *the_score += adjusted_score;
                    // token_hits.push(token_parentval_id);
                }
            }
        });

        // let ref parent_ids_for_token = token_kvdata.get[*value_id as usize];
        // trace!("value_id {:?}", value_id);
        // trace!("parent_ids_for_token {:?}", parent_ids_for_token);
    }
    debug!("checked {:?}, got {:?} token hits",hits.iter().count(), token_hits.iter().count());
    hits.extend(token_hits);
    // {
    //     debugTime!("token_hits.sort_by");
    //     token_hits.sort_by(|a, b| a.0.cmp(&b.0));
    // }
    // for hit in token_hits {
    //     hits.insert(hit, 1.5);
    // }
    // for hit in hits.iter() {
    //     trace!("NEW HITS {:?}", hit);
    // }

}



fn distance(s1: &str, s2: &str) -> u32 {
    let len_s1 = s1.chars().count();

    let mut column: Vec<u32> = Vec::with_capacity(len_s1+1);
    unsafe { column.set_len(len_s1+1); }
    for x in 0..len_s1+1 {
        column[x] = x as u32;
    }

    for (x, current_char2) in s2.chars().enumerate() {
        column[0] = x as u32  + 1;
        let mut lastdiag = x as u32;
        for (y, current_char1) in s1.chars().enumerate() {
            if current_char1 != current_char2 { lastdiag+=1 }
            let olddiag = column[y+1];
            column[y+1] = cmp::min(column[y+1]+1, cmp::min(column[y]+1, lastdiag));
            lastdiag = olddiag;

        }
    }
    column[len_s1]

}
