%start Expr
%avoid_insert "INT"
%%
Expr -> Result<Expr, ()>:
      SelectQuery { $1 }
    | InsertQuery { $1 }
    | UpdateQuery { $1 }
    | DeleteQuery { $1 }
    ;


Term -> Result<Expr, ()>:
      Term '*' Factor { Ok(Expr::Mul{ span: $span, lhs: Box::new($1?), rhs: Box::new($3?) }) }
    | Factor { $1 }
    ;

Factor -> Result<Expr, ()>:
      '(' Expr ')' { $2 }
    | 'INT' { Ok(Expr::Number{ span: $span }) }
    ;
%%

use cfgrammar::Span;

#[derive(Debug)]


/*

SelectQuery -> 'SELECT' Columns 'FROM' Table 'WHERE' 


SELECT *, column, column,
FROM table
WHERE column = 'a' OR column = 12345 OR column != 'a' AND column = 'a' OR (column = 'a' OR 'column' = b)
AND column < 'a' AND column >= 'b'

INSERT INTO table
VALUES ('a', 1)

DELETE FROM table
WHERE condition
*/

pub enum Expr {
    Add {
        span: Span,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
    },
    Mul {
        span: Span,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
    },
    Number {
        span: Span
    }
}
