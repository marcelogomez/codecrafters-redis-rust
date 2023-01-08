use std::convert::TryFrom;

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum RESPDataType {
    SimpleString,
    Error,
    Integer,
    BulkString,
    Array,
}

impl RESPDataType {
    fn from_bytes(bytes: &[u8]) -> ParseResult<(Self, &[u8])> {
        if bytes.is_empty() {
            return Err(ParseError::NotEnoughBytes);
        }

        Ok((Self::try_from(bytes[0])?, &bytes[1..]))
    }

    fn expect(self, bytes: &[u8]) -> ParseResult<&[u8]> {
        let (actual, bytes) = Self::from_bytes(bytes)?;
        if actual == self {
            Ok(bytes)
        } else {
            Err(ParseError::UnexpectedDataType(self, actual))
        }
    }
}

impl TryFrom<u8> for RESPDataType {
    type Error = ParseError;

    fn try_from(value: u8) -> ParseResult<Self> {
        match value {
            b'+' => Ok(Self::SimpleString),
            b'-' => Ok(Self::Error),
            b':' => Ok(Self::Integer),
            b'$' => Ok(Self::BulkString),
            b'*' => Ok(Self::Array),
            c => Err(ParseError::UnknownDataType(c as char)),
        }
    }
}

impl Into<u8> for RESPDataType {
    fn into(self) -> u8 {
        match self {
            Self::SimpleString => b'+',
            Self::Error => b'-',
            Self::Integer => b':',
            Self::BulkString => b'$',
            Self::Array => b'*',
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum ParseError {
    UnknownDataType(char),
    // (expected, actual)
    UnexpectedDataType(RESPDataType, RESPDataType),
    // bytes stream terminated before expected
    NotEnoughBytes,
    UnexpectedNonNumericCharacter(char),
    MissingCLRF,
    NegativeValueLength,
}

type ParseResult<T> = Result<T, ParseError>;

#[derive(Debug, PartialEq)]
enum RESPValue {
    Integer(i64),
    // TODO: deal with null strings
    BulkString(String),
    SimpleString(String),
    Error(String),
    Array(Vec<RESPValue>),
}

impl RESPValue {
    fn parse(bytes: &[u8]) -> ParseResult<(Self, &[u8])> {
        let (data_type, bytes) = RESPDataType::from_bytes(bytes)?;
        match data_type {
            RESPDataType::Integer => {
                let (i, bytes) = parse_integer_value(bytes)?;
                Ok((Self::Integer(i), bytes))
            }
            RESPDataType::BulkString => {
                let (s, bytes) = parse_bulk_string_inner(bytes)?;
                Ok((Self::BulkString(s), bytes))
            }
            RESPDataType::SimpleString => {
                let (s, bytes) = parse_simple_string_contents(bytes)?;
                Ok((Self::SimpleString(s), bytes))
            }
            RESPDataType::Error => {
                let (s, bytes) = parse_simple_string_contents(bytes)?;
                Ok((Self::Error(s), bytes))
            }
            RESPDataType::Array => {
                let (len, bytes) = parse_array_len(bytes)?;

                let mut values = vec![];
                let mut bytes = bytes;
                for _ in 0..len {
                    let (value, new_bytes) = RESPValue::parse(bytes)?;
                    values.push(value);
                    bytes = new_bytes;
                }

                Ok((RESPValue::Array(values), bytes))
            }
        }
    }
}

/// Takes in a stream of bytes that represent a RESP message
/// and turns it into a printable debug string that escapes all the special characters
pub fn resp_to_debug_str(bytes: impl IntoIterator<Item = u8>) -> String {
    let mut out = String::new();
    for byte in bytes {
        match byte {
            b'\r' => out.push_str("\\r"),
            b'\n' => out.push_str("\\n"),
            _ => out.push(byte as char),
        }
    }
    out
}

// Checks that bytes starts with a CLRF, returns the remaining bytes
fn validate_clrf(bytes: &[u8]) -> ParseResult<&[u8]> {
    match bytes {
        [b'\r', b'\n', rest @ ..] => Ok(rest),
        _ => Err(ParseError::MissingCLRF),
    }
}

// Parses a clrf terminated number, returns the remaining bytes
fn parse_integer_value(mut bytes: &[u8]) -> ParseResult<(i64, &[u8])> {
    let mut num: i64 = 0;

    // Deal with negative numbers
    let is_negative = bytes[0] == b'-';
    if is_negative {
        bytes = &bytes[1..];
    }

    while !bytes.is_empty() && bytes[0] != b'\r' {
        let digit = bytes[0];
        if digit < b'0' || digit > b'9' {
            return Err(ParseError::UnexpectedNonNumericCharacter(bytes[0] as char));
        }
        num = num * 10 + (digit - b'0') as i64;
        bytes = &bytes[1..];
    }

    if is_negative {
        num = -num;
    }

    Ok((num, validate_clrf(bytes)?))
}

fn parse_simple_string_contents(mut bytes: &[u8]) -> ParseResult<(String, &[u8])> {
    let mut s = String::new();
    while let Err(_) = validate_clrf(bytes) {
        s.push(bytes[0] as char);
        bytes = &bytes[1..];

        if bytes.is_empty() {
            return Err(ParseError::MissingCLRF);
        }
    }

    Ok((s, validate_clrf(bytes)?))
}

fn parse_bulk_string_inner(bytes: &[u8]) -> ParseResult<(String, &[u8])> {
    let (len, bytes) = parse_integer_value(&bytes)?;
    if len < 0 {
        return Err(ParseError::NegativeValueLength);
    }
    let len = len as usize;

    if bytes.len() <= len {
        return Err(ParseError::NotEnoughBytes);
    }

    let s: String = bytes[..len].iter().map(|&c| c as char).collect();
    Ok((s, validate_clrf(&bytes[len..])?))
}

pub fn parse_bulk_string(bytes: &[u8]) -> ParseResult<(String, &[u8])> {
    let bytes = RESPDataType::BulkString.expect(bytes)?;
    parse_bulk_string_inner(bytes)
}

fn parse_array_len(bytes: &[u8]) -> ParseResult<(usize, &[u8])> {
    let (len, bytes) = parse_integer_value(&bytes)?;
    if len < 0 {
        Err(ParseError::NegativeValueLength)
    } else {
        Ok((len as usize, bytes))
    }
}

pub fn parse_bulk_string_array(bytes: &[u8]) -> ParseResult<(Vec<String>, &[u8])> {
    let bytes = RESPDataType::Array.expect(bytes)?;
    let (len, mut bytes) = parse_array_len(bytes)?;

    let mut array = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let (str, new_bytes) = parse_bulk_string(bytes)?;
        array.push(str);
        bytes = new_bytes;
    }

    Ok((array, bytes))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_bulk_string() {
        assert_eq!(
            RESPValue::parse("$5\r\nhello\r\nrest".as_bytes()),
            Ok((
                RESPValue::BulkString("hello".to_string()),
                "rest".as_bytes()
            )),
        );

        assert_eq!(
            RESPValue::parse("$5\r\nhello\r\n".as_bytes()),
            Ok((RESPValue::BulkString("hello".to_string()), "".as_bytes())),
        );
    }

    #[test]
    fn test_parse_bulk_string_negative_len() {
        assert_eq!(
            RESPValue::parse("$-5\r\nhello\r\nrest".as_bytes()),
            Err(ParseError::NegativeValueLength),
        );
    }

    #[test]
    fn test_parse_bulk_string_len_missing_clrf() {
        assert_eq!(
            RESPValue::parse("$5hello\r\nrest".as_bytes()),
            Err(ParseError::UnexpectedNonNumericCharacter('h'))
        );
    }

    #[test]
    fn test_parse_bulk_string_not_enough_bytes() {
        assert_eq!(
            RESPValue::parse("$5\r\nhell".as_bytes()),
            Err(ParseError::NotEnoughBytes),
        );
    }

    #[test]
    fn test_parse_bulk_string_missing_clrf_termination() {
        assert_eq!(
            RESPValue::parse("$5\r\nhello".as_bytes()),
            Err(ParseError::NotEnoughBytes),
        );

        assert_eq!(
            RESPValue::parse("$5\r\nhelloooo".as_bytes()),
            Err(ParseError::MissingCLRF),
        );

        assert_eq!(
            RESPValue::parse("$5\r\nhelloooo\r\n".as_bytes()),
            Err(ParseError::MissingCLRF),
        );
    }

    #[test]
    fn test_parse_array() {
        assert_eq!(
            RESPValue::parse("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".as_bytes()),
            Ok((
                RESPValue::Array(vec![
                    RESPValue::BulkString("hello".to_string()),
                    RESPValue::BulkString("world".to_string()),
                ]),
                "".as_bytes()
            )),
        );

        assert_eq!(
            RESPValue::parse("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".as_bytes()),
            Ok((
                RESPValue::Array(vec![
                    RESPValue::BulkString("hello".to_string()),
                    RESPValue::BulkString("world".to_string()),
                ]),
                "".as_bytes()
            )),
        );
    }

    #[test]
    fn test_parse_mixed_array() {
        assert_eq!(
            RESPValue::parse("*4\r\n$5\r\nhello\r\n:123\r\n-ERROR\r\n+Simple\r\nrest".as_bytes()),
            Ok((
                RESPValue::Array(vec![
                    RESPValue::BulkString("hello".to_string()),
                    RESPValue::Integer(123),
                    RESPValue::Error("ERROR".to_string()),
                    RESPValue::SimpleString("Simple".to_string()),
                ]),
                "rest".as_bytes()
            )),
        );
    }

    #[test]
    fn test_parse_nested_array() {
        assert_eq!(
            // [bulk(hello), [123, [456, simple(Simple)]]]
            RESPValue::parse(
                "*2\r\n$5\r\nhello\r\n*2\r\n:123\r\n*2\r\n:456\r\n+Simple\r\nrest".as_bytes()
            ),
            Ok((
                RESPValue::Array(vec![
                    RESPValue::BulkString("hello".to_string()),
                    RESPValue::Array(vec![
                        RESPValue::Integer(123),
                        RESPValue::Array(vec![
                            RESPValue::Integer(456),
                            RESPValue::SimpleString("Simple".to_string()),
                        ])
                    ]),
                ]),
                "rest".as_bytes()
            )),
        );
    }

    #[test]
    fn test_parse_array_negative_len() {
        assert_eq!(
            RESPValue::parse("*-2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".as_bytes()),
            Err(ParseError::NegativeValueLength),
        );
    }

    #[test]
    fn test_parse_array_too_few_elements() {
        assert_eq!(
            RESPValue::parse("*2\r\n$5\r\nhello\r\n".as_bytes()),
            Err(ParseError::NotEnoughBytes),
        );
    }

    #[test]
    fn test_parse_array_malformed_element() {
        assert_eq!(
            RESPValue::parse("*2\r\n$5\r\nhelloooo\r\n$5\r\nworld\r\n".as_bytes()),
            Err(ParseError::MissingCLRF),
        );
    }

    #[test]
    fn test_parse_data_type() {
        assert_eq!(RESPDataType::try_from(b'+'), Ok(RESPDataType::SimpleString));
        assert_eq!(RESPDataType::try_from(b'-'), Ok(RESPDataType::Error));
        assert_eq!(RESPDataType::try_from(b':'), Ok(RESPDataType::Integer));
        assert_eq!(RESPDataType::try_from(b'$'), Ok(RESPDataType::BulkString));
        assert_eq!(RESPDataType::try_from(b'*'), Ok(RESPDataType::Array));
    }

    #[test]
    fn test_parse_data_type_error() {
        assert_eq!(
            RESPDataType::try_from(b'x'),
            Err(ParseError::UnknownDataType('x'))
        );
        assert_eq!(
            RESPDataType::try_from(b'a'),
            Err(ParseError::UnknownDataType('a'))
        );
    }

    fn do_test_data_type_to_byte(t: RESPDataType, expected: u8) {
        let actual: u8 = t.into();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_data_type_to_byte() {
        do_test_data_type_to_byte(RESPDataType::SimpleString, b'+');
        do_test_data_type_to_byte(RESPDataType::Error, b'-');
        do_test_data_type_to_byte(RESPDataType::Integer, b':');
        do_test_data_type_to_byte(RESPDataType::BulkString, b'$');
        do_test_data_type_to_byte(RESPDataType::Array, b'*');
    }

    #[test]
    fn test_parse_integer() {
        assert_eq!(
            RESPValue::parse(":123\r\n".as_bytes()),
            Ok((RESPValue::Integer(123), "".as_bytes())),
        );

        assert_eq!(
            RESPValue::parse(":-123\r\n".as_bytes()),
            Ok((RESPValue::Integer(-123), "".as_bytes())),
        );
    }

    #[test]
    fn test_parse_integer_error() {
        assert_eq!(
            RESPValue::parse(":123".as_bytes()),
            Err(ParseError::MissingCLRF),
        );

        assert_eq!(
            RESPValue::parse(":12l23\r\n".as_bytes()),
            Err(ParseError::UnexpectedNonNumericCharacter('l')),
        );
    }

    #[test]
    fn test_parse_simple_string() {
        assert_eq!(
            RESPValue::parse("+OK\r\n".as_bytes()),
            Ok((RESPValue::SimpleString("OK".to_string()), "".as_bytes())),
        );
    }

    #[test]
    fn test_parse_simple_string_with_intermediate_carriage_return() {
        assert_eq!(
            RESPValue::parse("+OK\rOK\r\n".as_bytes()),
            Ok((RESPValue::SimpleString("OK\rOK".to_string()), "".as_bytes())),
        );
    }

    #[test]
    fn test_parse_simple_string_error() {
        assert_eq!(
            RESPValue::parse("+OK".as_bytes()),
            Err(ParseError::MissingCLRF),
        );

        assert_eq!(
            RESPValue::parse("+OKOKOK\r".as_bytes()),
            Err(ParseError::MissingCLRF),
        );
    }

    #[test]
    fn test_parse_error() {
        assert_eq!(
            RESPValue::parse("-ERROR\r\n".as_bytes()),
            Ok((RESPValue::Error("ERROR".to_string()), "".as_bytes())),
        );
    }

    #[test]
    fn test_parse_error_with_intermediate_carriage_return() {
        assert_eq!(
            RESPValue::parse("-ERROR\rBAD\r\n".as_bytes()),
            Ok((RESPValue::Error("ERROR\rBAD".to_string()), "".as_bytes())),
        );
    }

    #[test]
    fn test_parse_error_error() {
        assert_eq!(
            RESPValue::parse("-ERROR".as_bytes()),
            Err(ParseError::MissingCLRF),
        );

        assert_eq!(
            RESPValue::parse("+ERROR\r".as_bytes()),
            Err(ParseError::MissingCLRF),
        );
    }
}
