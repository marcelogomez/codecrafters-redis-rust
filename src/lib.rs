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
    fn expect(self, bytes: &[u8]) -> ParseResult<&[u8]> {
        if bytes.is_empty() {
            return Err(ParseError::NotEnoughBytes);
        }

        let actual = Self::try_from(bytes[0])?;
        if actual == self {
            Ok(&bytes[1..])
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
fn parse_num_inner(mut bytes: &[u8]) -> ParseResult<(i64, &[u8])> {
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

fn parse_bulk_string_inner(bytes: &[u8], len: usize) -> ParseResult<(String, &[u8])> {
    if bytes.len() <= len {
        return Err(ParseError::NotEnoughBytes);
    }
    let s: String = bytes[..len].iter().map(|&c| c as char).collect();
    Ok((s, validate_clrf(&bytes[len..])?))
}

pub fn parse_bulk_string(bytes: &[u8]) -> ParseResult<(String, &[u8])> {
    let bytes = RESPDataType::BulkString.expect(bytes)?;
    let (len, bytes) = parse_num_inner(&bytes)?;
    if len < 0 {
        Err(ParseError::NegativeValueLength)
    } else {
        parse_bulk_string_inner(bytes, len as usize)
    }
}

fn parse_array_len(bytes: &[u8]) -> ParseResult<(usize, &[u8])> {
    let bytes = RESPDataType::Array.expect(bytes)?;
    let (len, bytes) = parse_num_inner(&bytes)?;
    if len < 0 {
        Err(ParseError::NegativeValueLength)
    } else {
        Ok((len as usize, bytes))
    }
}

pub fn parse_bulk_string_array(bytes: &[u8]) -> ParseResult<(Vec<String>, &[u8])> {
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
            parse_bulk_string("$5\r\nhello\r\nrest".as_bytes()),
            Ok(("hello".to_string(), "rest".as_bytes())),
        );

        assert_eq!(
            parse_bulk_string("$5\r\nhello\r\n".as_bytes()),
            Ok(("hello".to_string(), "".as_bytes())),
        );
    }

    #[test]
    fn test_parse_bulk_string_negative_len() {
        assert_eq!(
            parse_bulk_string("$-5\r\nhello\r\nrest".as_bytes()),
            Err(ParseError::NegativeValueLength),
        );
    }

    #[test]
    fn test_parse_bulk_string_len_missing_clrf() {
        assert_eq!(
            parse_bulk_string("$5hello\r\nrest".as_bytes()),
            Err(ParseError::UnexpectedNonNumericCharacter('h'))
        );
    }

    #[test]
    fn test_parse_bulk_string_not_enough_bytes() {
        assert_eq!(
            parse_bulk_string("$5\r\nhell".as_bytes()),
            Err(ParseError::NotEnoughBytes),
        );
    }

    #[test]
    fn test_parse_bulk_string_missing_clrf_termination() {
        assert_eq!(
            parse_bulk_string("$5\r\nhello".as_bytes()),
            Err(ParseError::NotEnoughBytes),
        );

        assert_eq!(
            parse_bulk_string("$5\r\nhelloooo".as_bytes()),
            Err(ParseError::MissingCLRF),
        );

        assert_eq!(
            parse_bulk_string("$5\r\nhelloooo\r\n".as_bytes()),
            Err(ParseError::MissingCLRF),
        );
    }

    #[test]
    fn test_parse_bulk_string_array() {
        assert_eq!(
            parse_bulk_string_array("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".as_bytes()),
            Ok((
                vec!["hello".to_string(), "world".to_string()],
                "".as_bytes()
            )),
        );

        assert_eq!(
            parse_bulk_string_array("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\nrest".as_bytes()),
            Ok((
                vec!["hello".to_string(), "world".to_string()],
                "rest".as_bytes()
            )),
        );
    }

    #[test]
    fn test_parse_bulk_string_array_negative_len() {
        assert_eq!(
            parse_bulk_string_array("*-2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".as_bytes()),
            Err(ParseError::NegativeValueLength),
        );
    }

    #[test]
    fn test_parse_bulk_string_array_too_few_elements() {
        assert_eq!(
            parse_bulk_string_array("*2\r\n$5\r\nhello\r\n".as_bytes()),
            Err(ParseError::NotEnoughBytes),
        );
    }

    #[test]
    fn test_parse_bulk_string_array_malformed_element() {
        assert_eq!(
            parse_bulk_string_array("*2\r\n$5\r\nhelloooo\r\n$5\r\nworld\r\n".as_bytes()),
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
}
