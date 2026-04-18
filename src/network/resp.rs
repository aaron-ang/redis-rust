use resp::Value;

/// Compute the RESP wire-format byte length of a Value without allocating.
pub fn resp_encoded_len(value: &Value) -> usize {
    match value {
        Value::Null | Value::NullArray => 5,
        Value::String(s) | Value::Error(s) => 1 + s.len() + 2,
        Value::Integer(i) => {
            let mut buf = itoa::Buffer::new();
            1 + buf.format(*i).len() + 2
        }
        Value::Bulk(s) => {
            let mut buf = itoa::Buffer::new();
            1 + buf.format(s.len()).len() + 2 + s.len() + 2
        }
        Value::BufBulk(v) => {
            let mut buf = itoa::Buffer::new();
            1 + buf.format(v.len()).len() + 2 + v.len() + 2
        }
        Value::Array(a) => {
            let mut buf = itoa::Buffer::new();
            1 + buf.format(a.len()).len() + 2 + a.iter().map(resp_encoded_len).sum::<usize>()
        }
    }
}

/// Encode a RESP Value directly into an existing buffer, avoiding intermediate allocation.
pub fn encode_into(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Null => out.extend_from_slice(b"$-1\r\n"),
        Value::NullArray => out.extend_from_slice(b"*-1\r\n"),
        Value::String(s) => {
            out.push(b'+');
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Value::Error(s) => {
            out.push(b'-');
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Value::Integer(i) => {
            out.push(b':');
            let mut buf = itoa::Buffer::new();
            out.extend_from_slice(buf.format(*i).as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Value::Bulk(s) => {
            out.push(b'$');
            let mut buf = itoa::Buffer::new();
            out.extend_from_slice(buf.format(s.len()).as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Value::BufBulk(v) => {
            out.push(b'$');
            let mut buf = itoa::Buffer::new();
            out.extend_from_slice(buf.format(v.len()).as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(v);
            out.extend_from_slice(b"\r\n");
        }
        Value::Array(a) => {
            out.push(b'*');
            let mut buf = itoa::Buffer::new();
            out.extend_from_slice(buf.format(a.len()).as_bytes());
            out.extend_from_slice(b"\r\n");
            for item in a {
                encode_into(item, out);
            }
        }
    }
}
