use std::{
    env::current_dir,
    path::PathBuf,
};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use clap::Parser;
use csv::Writer;
use serde::Serialize;
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt as _,
};

use crate::error::{Error, Result};


#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// The directory to write the CSV files to
    ///
    /// Defaults to the current directory
    #[arg(short, long, value_name = "DIR")]
    output_dir: Option<PathBuf>,
    /// The date to start scraping posts from in the format YYYY-MM-DD
    #[arg(
        short, 
        long, 
        value_name = "YYYY-MM-DD", 
        default_value_t = NaiveDate::from_ymd_opt(2023, 8, 1).unwrap()
    )]
    start_date: NaiveDate,
    /// The maximum number of times to retry a failed request
    #[arg(long, value_name = "NUM", default_value_t = 3u8)]
    pub max_retries: u8,
}

impl Cli {
    pub fn start_date(&self) -> NaiveDateTime {
        NaiveDateTime::new(
            self.start_date,
            NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        )
    }
}

impl Cli {
    pub async fn write_csv<I>(&self, records: I, filename: &str) -> Result<()>
    where
        I: IntoIterator,
        I::Item: Serialize,
    {
        let mut writer = Writer::from_writer(vec![]);

        for record in records {
            writer.serialize(record)?;
        }

        let data = writer.into_inner().map_err(|_| Error::CsvWriteFailed)?;

        let dir_path = self
            .output_dir
            .as_ref()
            .unwrap_or(&current_dir()?)
            .join("data");

        fs::create_dir_all(&dir_path).await?;

        let file_path = dir_path.join(filename);

        File::create(file_path)
            .await?
            .write_all(data.as_slice())
            .await
            .map_err(Into::into)
    }
}