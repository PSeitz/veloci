use crate::tokenizer::*;

/// This will group consecutive seperator tokens
/// "ok---nice" -> ["ok", "-", "-", "-", "nice"]
#[derive(Debug)]
pub struct SimpleTokenizer {}
impl Tokenizer for SimpleTokenizer {
    fn has_tokens(&self, text: &str) -> bool {
        // SEPERATORS.is_match(orignal)
        let mut iter = self.iter(text);
        iter.next();
        iter.next().is_some()
    }

    fn iter<'a>(&'a self, original: &'a str) -> Box<dyn Iterator<Item = (&'a str, bool)> + 'a> {
        Box::new(SimpleTokenIter::from_str(original))
    }
}

#[derive(Debug, Clone)]
pub struct SimpleTokenIter<'a> {
    original: &'a str,
    last_returned_byte: usize,
    last_was_token: bool,
    char_iter: std::str::CharIndices<'a>, // field_id: u8
}

impl<'a> SimpleTokenIter<'a> {
    pub fn from_str(text: &'a str) -> SimpleTokenIter<'a> {
        SimpleTokenIter {
            original: text,
            last_returned_byte: 0,
            last_was_token: false,
            char_iter: text.char_indices(),
        }
    }
}

impl<'a> Iterator for SimpleTokenIter<'a> {
    type Item = (&'a str, bool);

    #[inline]
    fn next(&mut self) -> Option<(&'a str, bool)> {
        for (char_byte_pos, char) in self.char_iter.by_ref() {
            if is_default_seperator(char) {
                if char_byte_pos == 0 {
                    self.last_was_token = true;
                } else {
                    let slice = (&self.original[self.last_returned_byte..char_byte_pos], false);
                    self.last_was_token = true;
                    self.last_returned_byte = char_byte_pos;
                    return Some(slice);
                }
            } else if self.last_was_token {
                let slice = (&self.original[self.last_returned_byte..char_byte_pos], true);
                self.last_was_token = false;
                self.last_returned_byte = char_byte_pos;
                return Some(slice);
            }
        }

        if self.last_returned_byte != self.original.len() {
            let slice = (&self.original[self.last_returned_byte..self.original.len()], self.last_was_token);
            self.last_returned_byte = self.original.len();
            Some(slice)
        } else {
            None
        }
    }
}

impl<'a> std::iter::FusedIterator for SimpleTokenIter<'a> {}
