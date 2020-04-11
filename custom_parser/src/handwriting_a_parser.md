Handwriting a Parser in Rust

#### Motivation
For my search engine I wanted to have a query language similar to what elastic search is using.
There are a lot of parser libraries I tested and implemented a solution on, two of them are pest and combine. Both are decent parsers, but in the end they lacked the flexibility, were hard to understand/write at some point and had really bad compile times.
So why not write an own parser, how hard can it be? Not that hard actually.

## Syntax

myField:(cool AND NICE) OR wordistance~2

    	 OR
   		/  	\
   myField
   	/   \



## Requirements/Nice to Have

#### Order of tokens should not be lost
This is important to keep information for phrase boosting

#### Meaningful Errors
If parsing fails it should not only exactly say what went wrong, it should also point where in the query is an error.

#### And/Or Precendence
Since I couldn't implement this in the other parsers, I will it ignore here for now. Extending the parser with some kind priority while parsing, or fixing the tree afterwards shouldn't be too hard though.

#### Flexibility
Something I would love to see would be to dynamically enable and disable parts of the syntax. Maybe someone doesn' want the "~" to define levenshtein distance on the tokens, for performance reasons. Or maybe using anthoer character for this.


## Divide and Conquery
I know actually almost nothing about the types parser like LR or shift-reduce, and I will not care about it here. I will just use the power of the brain to solve the issue, amazing right?

Splitting the logic into tokenizing and parsing seems reasonable, so let's just do that. 

#### Tokenizer
The tokenizer is quite powerful and can handle phrasing properly, since this will make things much easier than putting it into the grammar handling.  

#### Parser
The parser is recursive parser.
The grammar is realized as implementation, there is no meta layer describing the grammar and generating the parser code from it, like in pest.

The parser has zero dependencies. What I dislike most about rust are slow compile times, adding the fehler crate e.g. increases clean release compile times from 0,6s to 20s. Comparing how much time this saves in typing now vs. the accumulated wait in compile time, it's not worth it.
Currently the parser has a wonderful feedback cycle, since the watcher on the tests executes in around a second.

## Conclusion
I tried several parsers a was never happy with them, the documentation was hard to understand or lacking, and after some time usually limitations emerged, which limited the scope of the parser.
In pest this was incompatible handling of whitespace for phrases and in combine it was precendence handling. Conversion from the parse result to a custom AST was also rather hard in pest.

I see how these limitations emerge in a generic parser, but they are not an issue in the handwritten parser in this case.

I can recommend writing an own parser instead using a library.

### Complexity
I expected the implementation of the grammar to be monstrous, like the generated code from yac+bison. 
The lexer logic is around 100lines, the parser has around 110 lines, so 210 lines in total.

In comparison, the combine parser has also 210 lines, but the complexity is much higher because of the extreme nesting.
In pest I just implemented the grammar, but never the conversion to an AST, because it was just too painful.

In addition the handwritten version can handle much more complex grammar, so the comparability is not completely feasible.