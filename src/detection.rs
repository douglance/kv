use std::io::{self, IsTerminal, Read};
use std::path::Path;

#[derive(Debug)]
pub enum InputSource {
    Stdin(Vec<u8>),
    File { path: String, content: Vec<u8> },
    Literal(String),
}

impl InputSource {
    pub fn content(&self) -> &[u8] {
        match self {
            InputSource::Stdin(data) => data,
            InputSource::File { content, .. } => content,
            InputSource::Literal(s) => s.as_bytes(),
        }
    }

    pub fn original_filename(&self) -> Option<&str> {
        match self {
            InputSource::File { path, .. } => {
                Path::new(path).file_name().and_then(|s| s.to_str())
            }
            _ => None,
        }
    }

    pub fn content_type(&self) -> Option<&'static str> {
        match self {
            InputSource::Literal(_) => Some("text/plain"),
            InputSource::File { path, .. } => detect_content_type(path),
            InputSource::Stdin(_) => None,
        }
    }
}

fn detect_content_type(path: &str) -> Option<&'static str> {
    let ext = Path::new(path)
        .extension()
        .and_then(|s| s.to_str())
        .map(|s| s.to_lowercase());

    match ext.as_deref() {
        Some("json") => Some("application/json"),
        Some("txt") => Some("text/plain"),
        Some("md") => Some("text/markdown"),
        Some("xml") => Some("application/xml"),
        Some("html") | Some("htm") => Some("text/html"),
        Some("css") => Some("text/css"),
        Some("js") => Some("application/javascript"),
        Some("yaml") | Some("yml") => Some("application/yaml"),
        Some("toml") => Some("application/toml"),
        Some("csv") => Some("text/csv"),
        Some("png") => Some("image/png"),
        Some("jpg") | Some("jpeg") => Some("image/jpeg"),
        Some("gif") => Some("image/gif"),
        Some("pdf") => Some("application/pdf"),
        _ => None,
    }
}

pub fn detect_input(value: Option<&str>, literal: bool) -> io::Result<InputSource> {
    let stdin = io::stdin();

    // Only read from stdin if no value provided AND stdin is not a terminal (i.e., piped)
    if value.is_none() && !stdin.is_terminal() {
        let mut buffer = Vec::new();
        stdin.lock().read_to_end(&mut buffer)?;
        return Ok(InputSource::Stdin(buffer));
    }

    // Must have a value if stdin is not available
    let value = value.ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "no value provided")
    })?;

    // If literal flag is set, treat as string
    if literal {
        return Ok(InputSource::Literal(value.to_string()));
    }

    // Check if it's a file path
    let path = Path::new(value);
    if path.exists() && path.is_file() {
        let content = std::fs::read(path)?;
        return Ok(InputSource::File {
            path: value.to_string(),
            content,
        });
    }

    // Default to literal string
    Ok(InputSource::Literal(value.to_string()))
}
