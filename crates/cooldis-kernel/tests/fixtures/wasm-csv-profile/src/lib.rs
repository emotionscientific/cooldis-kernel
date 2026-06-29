use cooldis_guest_sdk::{
    OperationDefinition, OperationEventKind, OperationManifest, OperationMode, OperationValueKind,
    STATUS_INVALID_ARGUMENT, STATUS_NOT_FOUND, STATUS_OK, Sink, Source, StatusCode, read_source,
    write_sink,
};
use serde::{Deserialize, Serialize};

const CSV_PROFILE_ID: u32 = 1;

#[unsafe(no_mangle)]
pub extern "C" fn __cooldis_describe_module__(sink: u32) -> i32 {
    let manifest = OperationManifest::new(vec![OperationDefinition {
        id: CSV_PROFILE_ID,
        name: "csv_profile".to_string(),
        input: OperationValueKind::Json,
        output: OperationValueKind::Json,
        events: OperationEventKind::None,
        mode: OperationMode::Sync,
        required_capabilities: Vec::new(),
    }]);
    let bytes = match manifest.to_json_vec() {
        Ok(bytes) => bytes,
        Err(_) => return STATUS_INVALID_ARGUMENT,
    };
    status(write_sink(Sink(sink), &bytes).map(|_| ()))
}

#[unsafe(no_mangle)]
pub extern "C" fn __cooldis_call_operation__(
    operation: u32,
    _invocation: u32,
    source: u32,
    output: u32,
    _events: u32,
) -> i32 {
    match operation {
        CSV_PROFILE_ID => status(csv_profile(Source(source), Sink(output))),
        _ => STATUS_NOT_FOUND,
    }
}
#[derive(Deserialize)]
struct CsvProfileInput {
    csv: String,
    #[serde(default = "default_has_header")]
    has_header: bool,
}

#[derive(Serialize)]
struct CsvProfileOutput {
    rows: usize,
    columns: Vec<ColumnProfile>,
}

#[derive(Serialize)]
struct ColumnProfile {
    name: String,
    non_empty: usize,
    empty: usize,
    numeric_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    min: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mean: Option<f64>,
}

#[derive(Default)]
struct ColumnAccumulator {
    non_empty: usize,
    empty: usize,
    numeric_count: usize,
    min: Option<f64>,
    max: Option<f64>,
    sum: f64,
}

impl ColumnAccumulator {
    fn observe(&mut self, raw: &str) {
        let value = raw.trim();
        if value.is_empty() {
            self.empty += 1;
            return;
        }
        self.non_empty += 1;
        if let Ok(number) = value.parse::<f64>() {
            if number.is_finite() {
                self.numeric_count += 1;
                self.sum += number;
                self.min = Some(self.min.map_or(number, |current| current.min(number)));
                self.max = Some(self.max.map_or(number, |current| current.max(number)));
            }
        }
    }

    fn finish(self, name: String) -> ColumnProfile {
        ColumnProfile {
            name,
            non_empty: self.non_empty,
            empty: self.empty,
            numeric_count: self.numeric_count,
            min: self.min,
            max: self.max,
            mean: (self.numeric_count > 0).then_some(self.sum / self.numeric_count as f64),
        }
    }
}

fn csv_profile(source: Source, output: Sink) -> Result<(), StatusCode> {
    let input: CsvProfileInput =
        serde_json::from_slice(&read_all_source(source)?).map_err(|_| StatusCode::InvalidArgument)?;
    let rows = parse_csv_rows(&input.csv);
    let Some(first_row) = rows.first() else {
        let empty = CsvProfileOutput {
            rows: 0,
            columns: Vec::new(),
        };
        return write_json(output, &empty);
    };

    let column_count = first_row.len();
    let names = if input.has_header {
        first_row.clone()
    } else {
        (0..column_count)
            .map(|index| format!("column_{}", index + 1))
            .collect()
    };
    let data_rows = if input.has_header { &rows[1..] } else { &rows[..] };
    let mut columns = (0..column_count)
        .map(|_| ColumnAccumulator::default())
        .collect::<Vec<_>>();

    for row in data_rows {
        for index in 0..column_count {
            let value = row.get(index).map(String::as_str).unwrap_or("");
            columns[index].observe(value);
        }
    }

    let output_value = CsvProfileOutput {
        rows: data_rows.len(),
        columns: columns
            .into_iter()
            .enumerate()
            .map(|(index, column)| {
                column.finish(
                    names
                        .get(index)
                        .filter(|name| !name.trim().is_empty())
                        .cloned()
                        .unwrap_or_else(|| format!("column_{}", index + 1)),
                )
            })
            .collect(),
    };
    write_json(output, &output_value)
}

fn parse_csv_rows(input: &str) -> Vec<Vec<String>> {
    input
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            line.split(',')
                .map(|cell| cell.trim().to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn read_all_source(source: Source) -> Result<Vec<u8>, StatusCode> {
    let mut output = Vec::new();
    let mut buffer = [0u8; 1024];
    loop {
        let n = read_source(source, &mut buffer)?;
        if n == 0 {
            break;
        }
        output.extend_from_slice(&buffer[..n]);
        if n < buffer.len() {
            break;
        }
    }
    Ok(output)
}

fn write_json(output: Sink, value: &impl Serialize) -> Result<(), StatusCode> {
    let bytes = serde_json::to_vec(value).map_err(|_| StatusCode::InvalidArgument)?;
    write_sink(output, &bytes)?;
    Ok(())
}

fn default_has_header() -> bool {
    true
}

fn status(result: Result<(), StatusCode>) -> i32 {
    match result {
        Ok(()) => STATUS_OK,
        Err(err) => err.as_raw(),
    }
}
