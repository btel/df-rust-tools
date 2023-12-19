use clap::{Parser, ValueEnum};
use std::path::Path;
use polars::prelude::*;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    file_paths: Vec<String>,
    #[arg(short, long)]
    #[arg(short, value_enum)]
    apply: Vec<DefaultOps>,
}

#[derive(Clone, Debug)]
enum DefaultOps {
    Join(String),
    Summarize,
    Concat,
    Select(Vec<String>),
}
impl std::str::FromStr for DefaultOps {
    type Err = &'static str;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "summarize" => Ok(Self::Summarize),
            "concat" => Ok(Self::Concat),
            _ if s.starts_with("join:") => Ok(Self::Join(s.chars().skip(5).collect())),
            _ if s.starts_with("select:") => Ok(Self::Select(String::from_iter(s.chars().skip(7)).split(",").map(str::to_string).collect())),
            _ => unimplemented!()
        }
    }
}


fn read_csv(path: &String) -> PolarsResult<DataFrame> {
    let filename = Path::new(path).file_name().unwrap().to_str().unwrap();

    let df_csv = CsvReader::from_path(path)?
        .infer_schema(None)
        .has_header(true)
        .finish();
    df_csv
}

fn select(df: DataFrame, col_names: &Vec<String>) -> PolarsResult<DataFrame> {
    df.lazy().select(col_names.into_iter().map(|s: &String| -> Expr {col(s)}).collect::<Vec<Expr>>()).collect() //.name().suffix(&format!("_{}", filename))]).collect()
}

/// Join dataframes and return
fn join_dataframes(df_left: &DataFrame, df_right: &DataFrame, join_on: &str) -> PolarsResult<DataFrame> {
    let args = JoinArgs { how: JoinType::Inner, validation: Default::default(), suffix: None, slice: None };
    df_left.join(df_right, [join_on], [join_on], args)
}

fn concat_dataframes(dataframes: Vec<DataFrame>) -> PolarsResult<DataFrame> {
    concat(dataframes.into_iter().map(|df| df.lazy()).collect::<Vec<_>>(), UnionArgs::default()).unwrap().collect()
}

fn summarize(df: DataFrame) -> PolarsResult<DataFrame> {
    df.describe(None)
}

fn apply_op(df_inputs: Vec<DataFrame>, op: &DefaultOps) -> Vec<DataFrame> {

    let df: PolarsResult<Vec<DataFrame>> = match op {
        DefaultOps::Select(s) => df_inputs.into_iter().map(|df| {select(df, s)}).collect(),
        DefaultOps::Summarize => df_inputs.into_iter().map(summarize).collect(),
        DefaultOps::Join(on) => {
            let mut retvals = Vec::new();

            let joined_df = df_inputs.into_iter().reduce(|acc, df| join_dataframes(&acc, &df, on).unwrap()).unwrap();
            retvals.push(joined_df);
            Ok(retvals)
        },
        DefaultOps::Concat => Ok(vec![concat_dataframes(df_inputs).unwrap()]),
        _ => Ok(df_inputs)
    };
    df.expect(&format!("error applying operation {:?}", op))
}
fn main() {
    let cli = Cli::parse();
    let mut df_inputs: Vec<DataFrame> = cli.file_paths.iter().map(read_csv).collect::<PolarsResult<Vec<DataFrame>>>().expect("error reading files");

    for op in cli.apply.iter() {
        df_inputs = apply_op(df_inputs, op)
    }
    println!("{}", df_inputs.first().unwrap())
    // println!("{:?}", &cli.apply);
    // let df_joined = .reduce(|acc, df| join_dataframes(&acc, &df, "index").unwrap());
    // let df_csv = add_summary(df_joined.unwrap());
    // println!("{}", df_csv.unwrap());
}
