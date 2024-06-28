grammar ZetaSQLTypeGrammar;

// Parser rules
type : basicType | arrayType | structType;
basicType: BASIC_TYPE ( '(' typeParameters ')' )?;
arrayType: ARRAY '<' type '>';
structType: STRUCT '<' structFields '>';
structFields: structField (',' ' '* structField)*;
structField: IDENTIFIER ' ' type;
typeParameters: TYPE_PARAMETER (',' ' '* TYPE_PARAMETER)*;

// Lexer rules
TYPE_PARAMETER: NUMBER | 'MAX';

ARRAY: 'ARRAY';
STRUCT: 'STRUCT';

BASIC_TYPE: STRING
            | BYTES
            | INT32
            | INT64
            | UINT32
            | UINT64
            | FLOAT64
            | DECIMAL
            | NUMERIC
            | BIGNUMERIC
            | INTERVAL
            | BOOL
            | TIMESTAMP
            | DATE
            | TIME
            | DATETIME
            | GEOGRAPHY
            | JSON;

STRING: S T R I N G;
BYTES: B Y T E S;
INT32: I N T THIRTYTWO;
INT64: I N T SIXTYFOUR;
UINT32: U I N T THIRTYTWO;
UINT64: U I N T SIXTYFOUR;
FLOAT64: F L O A T SIXTYFOUR;
DECIMAL: D E C I M A L;
NUMERIC: N U M E R I C;
BIGNUMERIC: B I G N U M E R I C;
INTERVAL: I N T E R V A L;
BOOL: B O O L;
TIMESTAMP: T I M E S T A M P;
DATE: D A T E;
TIME: T I M E;
DATETIME: D A T E T I M E;
GEOGRAPHY: G E O G R A P H Y;
JSON: J S O N;

IDENTIFIER: [A-Za-z_][A-Za-z0-9_]*;
NUMBER: [1-9][0-9]*;

fragment THIRTYTWO: '32';
fragment SIXTYFOUR: '64';
fragment A: [aA];
fragment B: [bB];
fragment C: [cC];
fragment D: [dD];
fragment E: [eE];
fragment F: [fF];
fragment G: [gG];
fragment H: [hH];
fragment I: [iI];
fragment J: [jJ];
fragment K: [kK];
fragment L: [lL];
fragment M: [mM];
fragment N: [nN];
fragment O: [oO];
fragment P: [pP];
fragment Q: [qQ];
fragment R: [rR];
fragment S: [sS];
fragment T: [tT];
fragment U: [uU];
fragment V: [vV];
fragment W: [wW];
fragment X: [xX];
fragment Y: [yY];
fragment Z: [zZ];
