Handwriting a Parser in Rust

## Motivation
For my search engine I wanted to have a query language similar to what elastic search is using.
There are a lot of parser libraries, and I tested solutions on several of them, two of them are pest and combine. Both are decent parsers, but the documentation was hard to understand or lacking, and after some time limitations emerged which limited the scope of the parser.
In pest this is incompatible handling of whitespace for phrases and in combine it is precendence handling. Conversion from the parse result to a custom AST is also rather hard in pest.
Another downside are the super slow compile times.

So why not write an own parser, how hard can it be? Not that hard actually.

## Requirements

#### Syntax
A short example to explain the syntax

myField:(cool AND NICE) OR fuzzyserachterm~2

    	       OR
   		    /  	   \
   Attr:myField
   	/   
  AND
  / \

We would get results, where we have a hit for cool AND nice in the myField property or fuzzyserachterm in all fields, but with an edit distance of 2. This syntax should be parsed into a tree, incomplete sketched above (incomplete, because ASCII art by hand is no fun).


#### Nice to Have Features

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
I know actually almost nothing about the parser theory like LR or shift-reduce, and I will not care about it here. I will just use the power of the brain.
Splitting the logic into tokenizing and parsing seems reasonable, so let's just do that. 

#### Tokenizer
The tokenizer is probably not a textbook tokenizer, because it is quite powerful.
Since it makes things so much easier, it will handle parts of the grammar, like phrasing and attributed blocks.

As an example for "name:fred" the lexer will emit [AttributedLiteral("name"), Literal("fred")], the colon will be omitted. Handling this in grammer part of the parser is much more complex.
It is also a not completetely strict, e.g. "AND AND AND" would be emitted as [Literal, And, Literal] since it doesn't make sense otherwise.

#### Parser
The parser is a recursive parser.
The grammar is realized as implementation, there is no meta layer describing the grammar and generating the parser code from it, like in pest.

## Conclusion


### Performance
In this case performane doesn't really matter, since parsing will probably always be fast enough. That said, let's check the results:

### Compile Times
Clean Release Compile Times are 0,8s. With combine they are 20s.

The parser has zero dependencies. What I dislike most about rust are slow compile times, adding the fehler crate e.g. increases clean release compile times from 0,8s to 20s. Comparing how much time this saves in typing now vs. the accumulated wait in compile time, it's not worth it.
Currently the parser has a wonderful feedback cycle, since the watcher on the tests executes instantly.

### Complexity
I expected the implementation of the grammar to be monstrous, like the generated code from yac+bison. 
The lexer logic is around 130 lines, the parser has around 150 lines, so 280 lines in total.

In comparison, the combine parser has 150 lines, but the complexity is quite high because of the nesting.
It should be noted that the combine parser itself has several thousands LOC.
In pest I just implemented the grammar, but never the conversion to an AST, because it was just too painful.

Comparability is not completely given, since the handwritten version can handle more complex grammar and has proper error handling.

For example compare the leaf handling in the parsers:

#### Combine
```rust
parser! {
    pub fn parse_to_ast[I]()(I) -> UserAST
    where [I: Stream<Item = char>]
    {
        
            attempt(
                chainl1(
                    leaf(),
                    parse_operator().map(|op: Operator|
                        move |left: UserAST, right: UserAST| {
                            combine_if_same_op!(left, op, right);
                            combine_if_same_op!(right, op, left);
                            UserAST::Clause(op, vec![left, right])
                        }
                    )
                )
            )
        
    }
}

```

#### Handwritten

```rust

fn try_parse_user_filter(&mut self, curr_token: Token) -> Result<UserFilter<'b>, ParseError> {
    let mut curr_ast = UserFilter {
        levenshtein: None,
        phrase: get_text_for_token(self.text, curr_token.byte_start_pos, curr_token.byte_stop_pos),
    };

    // Optional: Define Levenshtein distance
    if self.is_type(TokenType::Tilde) {
        self.next_token().unwrap(); // Remove Tilde

        self.assert_allowed_types("Expecting a levenshtein number after a \'~\' ", &[Some(TokenType::Literal)])?;

        let lev_token = self.next_token().unwrap(); // Remove levenshtein number
        let levenshtein: u8 = get_text_for_token(self.text, lev_token.byte_start_pos, lev_token.byte_stop_pos)
            .parse()
            .map_err(|_e| ParseError::ExpectedNumber(format!("Expected number after tilde to define levenshtein distance but got {:?}", lev_token)))?;
        curr_ast.levenshtein = Some(levenshtein);
    }
    Ok(curr_ast)
}

```

### Final

I can recommend writing an own parser instead using a library.


# Appendix - Performance

After optimization, the custom parser is 8x faster than the combine parser, and even the dumb clone everything version is 4x faster.

### Custom Parser Optimzation Steps
Base Line: Copy all texts from lexer to parser, and parser to ast
test tests::bench_lexer_long   ... bench:       1,814 ns/iter (+/- 743)
test tests::bench_lexer_medium ... bench:       1,195 ns/iter (+/- 103)
test tests::bench_lexer_short  ... bench:         222 ns/iter (+/- 13)
test tests::bench_parse_long   ... bench:       5,162 ns/iter (+/- 467)
test tests::bench_parse_medium ... bench:       2,231 ns/iter (+/- 192)
test tests::bench_parse_short  ... bench:         429 ns/iter (+/- 31)

Copy texts only from lexer to parser, by using VecDequeue and pop Tokens to gain ownership on the token text and mvoe them to the ast
test tests::bench_lexer_long   ... bench:       2,070 ns/iter (+/- 304)
test tests::bench_lexer_medium ... bench:       1,170 ns/iter (+/- 90)
test tests::bench_lexer_short  ... bench:         268 ns/iter (+/- 42)
test tests::bench_parse_long   ... bench:       4,411 ns/iter (+/- 826)
test tests::bench_parse_medium ... bench:       2,146 ns/iter (+/- 249)
test tests::bench_parse_short  ... bench:         402 ns/iter (+/- 46)


reference text instead of token with copy of text
test tests::bench_lexer_long   ... bench:         829 ns/iter (+/- 149)
test tests::bench_lexer_medium ... bench:         501 ns/iter (+/- 153)
test tests::bench_lexer_short  ... bench:         175 ns/iter (+/- 15)
test tests::bench_parse_long   ... bench:       4,150 ns/iter (+/- 684)
test tests::bench_parse_medium ... bench:       1,706 ns/iter (+/- 317)
test tests::bench_parse_short  ... bench:         355 ns/iter (+/- 142)


use vec and don't pop
- Previously a VecDequeue was used and the front tokens were removed to gain ownership on the token text
test tests::bench_lexer_long   ... bench:         865 ns/iter (+/- 348)
test tests::bench_lexer_medium ... bench:         497 ns/iter (+/- 129)
test tests::bench_lexer_short  ... bench:         186 ns/iter (+/- 24)
test tests::bench_parse_long   ... bench:       3,938 ns/iter (+/- 535)
test tests::bench_parse_medium ... bench:       1,372 ns/iter (+/- 279)
test tests::bench_parse_short  ... bench:         365 ns/iter (+/- 61)

reference text instead of copy in resulting AST
test tests::bench_lexer_long   ... bench:         823 ns/iter (+/- 105)
test tests::bench_lexer_medium ... bench:         489 ns/iter (+/- 41)
test tests::bench_lexer_short  ... bench:         227 ns/iter (+/- 16)
test tests::bench_parse_long   ... bench:       3,340 ns/iter (+/- 443)
test tests::bench_parse_medium ... bench:       1,153 ns/iter (+/- 172)
test tests::bench_parse_short  ... bench:         341 ns/iter (+/- 107)

u32 instead of usize
test tests::bench_lexer_long   ... bench:         706 ns/iter (+/- 113)
test tests::bench_lexer_medium ... bench:         490 ns/iter (+/- 55)
test tests::bench_lexer_short  ... bench:         182 ns/iter (+/- 20)
test tests::bench_parse_long   ... bench:       2,927 ns/iter (+/- 637)
test tests::bench_parse_medium ... bench:       1,089 ns/iter (+/- 125)
test tests::bench_parse_short  ... bench:         288 ns/iter (+/- 42)

remove unused u32 tokens char_pos in tokens
the 
test tests::bench_lexer_long   ... bench:         785 ns/iter (+/- 187)
test tests::bench_lexer_medium ... bench:         536 ns/iter (+/- 230)
test tests::bench_lexer_short  ... bench:         223 ns/iter (+/- 43)
test tests::bench_parse_long   ... bench:       2,262 ns/iter (+/- 208)
test tests::bench_parse_medium ... bench:       1,255 ns/iter (+/- 371)
test tests::bench_parse_short  ... bench:         288 ns/iter (+/- 25)

### Combine

test tests::bench_parse_long   ... bench:      18,991 ns/iter (+/- 1,529)
test tests::bench_parse_medium ... bench:       9,054 ns/iter (+/- 1,146)
test tests::bench_parse_short  ... bench:       2,126 ns/iter (+/- 191)