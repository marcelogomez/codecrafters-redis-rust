/// Takes in a stream of bytes that represent a RESP message
/// and turns it into a printable debug string that escapes all the special characters
pub fn resp_to_debug_str(bytes: impl IntoIterator<Item = u8>) -> String {
    bytes
        .into_iter()
        .map(|byte| match byte {
            b'\r' => "\\r".to_string(),
            b'\n' => "\\n".to_string(),
            _ => format!("{}", byte as char),
        })
        .collect()
}
