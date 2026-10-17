use std::path::PathBuf;

use csv_core::WriteResult;

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum Format {
    Table,
    Jsonl,
    Csv,
}

#[derive(clap::Parser)]
#[group(id = "output::Args")]
pub struct Args {
    #[arg(long, short = 'o')]
    output: Option<PathBuf>,
    #[arg(long, short='f', value_enum, default_value_t = Format::Table)]
    format: Format,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("could not open the specified output file at {1:?}")]
    OpenOutputFile(#[source] std::io::Error, PathBuf),
    #[error("could not write data to the output file at {1:?}")]
    WriteFile(#[source] std::io::Error, PathBuf),
    #[error("could not write data to the terminal")]
    WriteStdout(#[source] std::io::Error),
    #[error("could not serialize registers to JSON")]
    SerializeJson(#[source] serde_json::Error),
    #[error("could not serialize registers to CSV")]
    SerializeCsv(#[source] std::io::Error),
}

impl Args {
    pub fn to_output(self) -> Result<Output, Error> {
        let io = match &self.output {
            None => Box::new(std::io::stdout().lock()) as Box<_>,
            Some(path) => Box::new(
                std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(path)
                    .map_err(|e| Error::OpenOutputFile(e, path.clone()))?,
            ) as Box<_>,
        };
        let formatter = match &self.format {
            Format::Table => {
                let mut comfy = comfy_table::Table::new();
                comfy.set_content_arrangement(comfy_table::ContentArrangement::Dynamic);
                Formatter::Table { comfy: Box::new(comfy) }
            }
            Format::Jsonl => Formatter::Jsonl,
            Format::Csv => Formatter::Csv { written_records: false },
        };
        Ok(Output { args: self, io, formatter })
    }
}

pub struct Output {
    args: Args,
    io: Box<dyn std::io::Write>,
    formatter: Formatter,
}

enum Formatter {
    Csv { written_records: bool },
    Table { comfy: Box<comfy_table::Table> },
    Jsonl,
}

impl Output {
    pub fn table_headers(&mut self, hdrs: Vec<&'static str>) -> Result<(), Error> {
        match &mut self.formatter {
            Formatter::Csv { written_records } => {
                if *written_records {
                    panic!("table headers for csv must be written very first!");
                }
                *written_records = true;
                self.write_csv_row(&hdrs)?;
            }
            Formatter::Table { comfy } => {
                comfy.set_header(hdrs);
            }
            Formatter::Jsonl => {}
        }
        Ok(())
    }

    fn write_csv_row<V: std::ops::Deref<Target = str>>(
        &mut self,
        values: &[V],
    ) -> Result<(), Error> {
        let max_len = 2 + 2 * values.iter().map(|v| v.len()).max().unwrap_or(0);
        let mut output = vec![0; max_len];
        let mut writer = csv_core::Writer::new();
        for value in values {
            let inp = value.as_bytes();
            let (WriteResult::InputEmpty, ib, ob) = writer.field(inp, &mut output) else {
                panic!("something wrong with csv output");
            };
            assert_eq!(value.len(), ib);
            self.io.write_all(&output[..ob]).map_err(|e| self.write_error(e))?;
            let (WriteResult::InputEmpty, ob) = writer.delimiter(&mut output) else {
                panic!("something wrong with csv output");
            };
            self.io.write_all(&output[..ob]).map_err(|e| self.write_error(e))?;
        }
        let (WriteResult::InputEmpty, ob) = writer.terminator(&mut output) else {
            panic!("something wrong with csv output");
        };
        self.io.write_all(&output[..ob]).map_err(|e| self.write_error(e))
    }

    pub fn result<R: serde::Serialize>(
        &mut self,
        table_row: impl FnOnce() -> Vec<String>,
        serde_record: impl FnOnce() -> R,
    ) -> Result<(), Error> {
        match &mut self.formatter {
            Formatter::Csv { written_records } => {
                *written_records = true;
                let values = table_row();
                self.write_csv_row(&values)?;
            }
            Formatter::Table { comfy } => {
                comfy.add_row(table_row());
            }
            Formatter::Jsonl => {
                serde_json::to_writer(&mut self.io, &serde_record())
                    .map_err(Error::SerializeJson)?;
                writeln!(self.io).map_err(|e| self.write_error(e))?
            }
        }
        Ok(())
    }

    fn write_error(&self, e: std::io::Error) -> Error {
        match &self.args.output {
            None => Error::WriteStdout(e),
            Some(p) => Error::WriteFile(e, p.into()),
        }
    }

    pub fn commit(mut self) -> Result<(), Error> {
        match &self.formatter {
            Formatter::Csv { written_records: _ } => {}
            Formatter::Table { comfy } => {
                self.io.write_fmt(format_args!("{}", comfy)).map_err(|e| self.write_error(e))?;
            }
            Formatter::Jsonl => {}
        }
        self.io.flush().map_err(|e| self.write_error(e))
    }
}
