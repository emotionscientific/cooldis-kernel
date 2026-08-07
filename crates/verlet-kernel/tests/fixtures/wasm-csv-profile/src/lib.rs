const CSV_PROFILE_ID: u32 = 1;

#[unsafe(no_mangle)]
pub extern "C" fn __verlet_describe_module__(sink: u32) -> i32 {
    let manifest =
        verlet_guest_sdk::OperationManifest::new(vec![verlet_guest_sdk::OperationDefinition {
            id: CSV_PROFILE_ID,
            name: "csv_profile".to_string(),
            input: verlet_guest_sdk::OperationValueKind::Json,
            output: verlet_guest_sdk::OperationValueKind::Json,
            events: verlet_guest_sdk::OperationEventKind::None,
            mode: verlet_guest_sdk::OperationMode::Sync,
            required_capabilities: Vec::new(),
        }]);
    let bytes = match manifest.to_json_vec() {
        Ok(bytes) => bytes,
        Err(_) => return verlet_guest_sdk::STATUS_INVALID_ARGUMENT,
    };
    status(verlet_guest_sdk::write_sink(verlet_guest_sdk::Sink(sink), &bytes).map(|_| ()))
}

#[unsafe(no_mangle)]
pub extern "C" fn __verlet_call_operation__(
    operation: u32,
    _invocation: u32,
    source: u32,
    output: u32,
    _events: u32,
) -> i32 {
    match operation {
        CSV_PROFILE_ID => status(csv_profile(
            verlet_guest_sdk::Source(source),
            verlet_guest_sdk::Sink(output),
        )),
        _ => verlet_guest_sdk::STATUS_NOT_FOUND,
    }
}
#[derive(serde::Deserialize)]
struct CsvProfileInput {
    csv: String,
    #[serde(default = "default_has_header")]
    has_header: bool,
}

#[derive(serde::Serialize)]
struct CsvProfileOutput {
    rows: usize,
    columns: Vec<ColumnProfile>,
}

#[derive(serde::Serialize)]
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

fn csv_profile(
    source: verlet_guest_sdk::Source,
    output: verlet_guest_sdk::Sink,
) -> Result<(), verlet_guest_sdk::StatusCode> {
    let input: CsvProfileInput = serde_json::from_slice(&read_all_source(source)?)
        .map_err(|_| verlet_guest_sdk::StatusCode::InvalidArgument)?;
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
    let data_rows = if input.has_header {
        &rows[1..]
    } else {
        &rows[..]
    };
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

fn read_all_source(
    source: verlet_guest_sdk::Source,
) -> Result<Vec<u8>, verlet_guest_sdk::StatusCode> {
    let mut output = Vec::new();
    let mut buffer = [0u8; 1024];
    loop {
        let n = verlet_guest_sdk::read_source(source, &mut buffer)?;
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

fn write_json(
    output: verlet_guest_sdk::Sink,
    value: &impl serde::Serialize,
) -> Result<(), verlet_guest_sdk::StatusCode> {
    let bytes =
        serde_json::to_vec(value).map_err(|_| verlet_guest_sdk::StatusCode::InvalidArgument)?;
    verlet_guest_sdk::write_sink(output, &bytes)?;
    Ok(())
}

fn default_has_header() -> bool {
    true
}

fn status(result: Result<(), verlet_guest_sdk::StatusCode>) -> i32 {
    match result {
        Ok(()) => verlet_guest_sdk::STATUS_OK,
        Err(err) => err.as_raw(),
    }
}
