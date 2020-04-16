Handwriting a Parser in Rust

#### Motivation
For my search engine I wanted to have a query language similar to what elastic search is using.
There are a lot of parser libraries I tested and implemented solutions on, two of them are pest and combine. Both are decent parsers, but in the end they lacked the flexibility, were hard to understand/write at some point and had really bad compile times.
So why not write an own parser, how hard can it be? Not that hard actually.

## Requirements/Nice to Have

#### Syntax
A short summary on this small example, about the syntax

myField:(cool AND NICE) OR findthisplease~2

    	 OR
   		/  	\
   myField
   	/   \

We would get results, where we have a hit for cool AND nice int he myField property or findthisplease, but with an edit distance of 2, so findthisplea would also match. This syntax should be parsed into a tree, incomplete sketched above (incomplete, because ASCII art by hand is no fun).




- Order of tokens should not be lost
This is important to keep information for phrase boosting

- Meaningful Errors
If parsing fails it should not only exactly say what went wrong, it should also point where in the query is an error.

- And/Or Precendence
Since I couldn't implement this in the other parsers, I will it ignore here for now. Extending the parser with some kind priority while parsing, or fixing the tree afterwards shouldn't be too hard though.
This means "cool OR nice AND superb" is currently the same as "(cool OR nice) AND superb"

- Flexibility
Something I would love to see would be to dynamically enable and disable parts of the syntax. Maybe someone doesn' want the "~" to define levenshtein distance on the tokens, for performance reasons. Or maybe using another character for this.


## Divide and Conquery
I know actually almost nothing about the parser theory like LR or shift-reduce, and I will not care about it here. I will just use the power of the brain to solve the issue, amazing right?

Splitting the logic into tokenizing and parsing seems reasonable, so let's just do that. 

#### Tokenizer
The tokenizer is probably not a textbook tokenizer, because it is quite powerful.
Since it makes things so much easier, it will handle parts of the grammar, like phrasing and attributed blocks.

As an example for "name:fred" the lexer will emit [AttributedLiteral("name"), Literal("fred")], the colon will be omitted. Handling this in grammer part of the parser is much more complex.


#### Parser
The parser is a recursive parser.
The grammar is realized as implementation, there is no meta layer describing the grammar and generating the parser code from it, like in pest.

The parser has zero dependencies. What I dislike most about rust are slow compile times, adding the fehler crate e.g. increases clean release compile times from 0,6s to 20s. Comparing how much time this saves in typing now vs. the accumulated wait in compile time, it's not worth it.
Currently the parser has a wonderful feedback cycle, since the watcher on the tests executes in around a second.

## Conclusion
I tried several parsers a was never happy with them, the documentation was hard to understand or lacking, and after some time usually limitations emerged, which limited the scope of the parser.
In pest this was incompatible handling of whitespace for phrases and in combine it was precendence handling. Conversion from the parse result to a custom AST was also rather hard in pest.

I see how these limitations emerge in a generic parser, but they are not an issue in the handwritten parser in this case.

I can recommend writing an own parser instead using a library.

### Performance
In this case performane doesn't really matter, since parsing will always be fast enough. That said, let's check the results:


### Complexity
I expected the implementation of the grammar to be monstrous, like the generated code from yac+bison. 
The lexer logic is around 100 lines, the parser has around 180 lines, so 280 lines in total.

In comparison, the combine parser has 210 lines, but the complexity is much higher because of the extreme nesting.
It should be noted that the combine parser itself has several thousands LOC.
In pest I just implemented the grammar, but never the conversion to an AST, because it was just too painful.

Comparability is not completely given, since the handwritten version can handle much more complex grammar and has proper error handling.
Phrases, fields on groups.

For example compare the leaf handling in the parsers:

#### Combine
```rust
parser! {
    fn user_literal[I]()(I) -> UserAST
    where [I: Stream<Item = char>]
    {
        let term_val = || {
            let phrase = (char('"'), many1(satisfy(|c| c != '"')), char('"')).map(|(_, s, _)| s);
            phrase.or(word())
        };
        let term_with_field_and_levenshtein =
            (field(), char(':'), term_val(), char('~'), digit()).map(|(field_name, _, phrase, _, digit)| UserFilter {
                field_name: Some(field_name),
                phrase,
                levenshtein: Some(digit.to_string().parse::<u8>().unwrap())
            }.into_ast());
        let term_with_field =
            (field(), char(':'), term_val()).map(|(field_name, _, phrase)| UserFilter {
                field_name: Some(field_name),
                phrase,
                levenshtein: None
            }.into_ast());
        let term_no_field_and_levenshtein =
            (term_val(), char('~'), digit()).map(|(phrase, _, digit)| UserFilter {
                field_name: None,
                phrase,
                levenshtein: Some(digit.to_string().parse::<u8>().unwrap())
            }.into_ast());
        let term_no_field = term_val().map(|phrase| UserFilter {
            field_name: None,
            phrase,
            levenshtein: None
        }.into_ast());

        attempt(term_with_field_and_levenshtein)
            .or(attempt(term_no_field_and_levenshtein))
            .or(attempt(term_with_field))
            .or(term_no_field)
    }
}

```

#### Handwritten

```rust

fn try_parse_usesr_filter(&mut self) -> Result<Option<UserFilter>, ParseError> {

    self.tokens.get(self.pos).cloned().map(|curr_token|{
        let mut curr_ast = UserFilter {
            levenshtein: None,
            phrase: curr_token.matched_text.to_string(),
        };

        // Define Levenshtein distance
        if self.is_type(self.pos + 1, TokenType::Tilde) {
            let levenshtein: u8 = self.parse_after_tilde(self.pos + 2)?;
            curr_ast.levenshtein = Some(levenshtein);

            self.pos += 2; // e.g. House~3 -> tokens [~], [3]
        }
        Ok(curr_ast)
    }).transpose()
}


```