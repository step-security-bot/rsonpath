use crate::input::{self, FileOrStdin, ResolvedInputKind};
use crate::{
    args::{EngineArg, InputArg, ResultArg},
    error::report_engine_error,
};
use eyre::{Result, WrapErr};
use log::warn;
use rsonpath_lib::{
    engine::{error::EngineError, main::MainEngine, recursive::RecursiveEngine, Compiler, Engine},
    input::{BufferedInput, Input, MmapInput, OwnedBytes},
    query::automaton::Automaton,
    result::{CountResult, IndexResult},
};
use std::{
    fs,
    io::{self, Read},
    path::Path,
};

pub struct Runner<'q> {
    pub with_compiled_query: Automaton<'q>,
    pub with_engine: ResolvedEngine,
    pub with_input: ResolvedInput,
    pub with_output: ResolvedOutput,
}

impl<'q> Runner<'q> {
    pub fn run(self) -> Result<()> {
        match self.with_engine {
            ResolvedEngine::Recursive => {
                let engine = RecursiveEngine::from_compiled_query(self.with_compiled_query);
                self.with_input
                    .run_engine(engine, self.with_output)
                    .wrap_err("Error running the recursive engine.")
            }
            ResolvedEngine::Main => {
                let engine = MainEngine::from_compiled_query(self.with_compiled_query);
                self.with_input
                    .run_engine(engine, self.with_output)
                    .wrap_err("Error running the main engine.")
            }
        }
    }
}

pub fn resolve_input<P: AsRef<Path>>(file_path: Option<P>, force_input: Option<&InputArg>) -> Result<ResolvedInput> {
    let file = match file_path {
        Some(path) => FileOrStdin::File(fs::File::open(path).wrap_err("Error reading the provided file.")?),
        None => FileOrStdin::Stdin(io::stdin()),
    };

    let (kind, fallback_kind) = input::decide_input_strategy(&file, force_input)?;

    Ok(ResolvedInput {
        file,
        kind,
        fallback_kind,
    })
}

pub fn resolve_output(result_arg: ResultArg) -> ResolvedOutput {
    match result_arg {
        ResultArg::Bytes => ResolvedOutput::Index,
        ResultArg::Count => ResolvedOutput::Count,
    }
}

pub fn resolve_engine(engine_arg: EngineArg) -> ResolvedEngine {
    match engine_arg {
        EngineArg::Main => ResolvedEngine::Main,
        EngineArg::Recursive => ResolvedEngine::Recursive,
    }
}

pub enum ResolvedEngine {
    Recursive,
    Main,
}

pub struct ResolvedInput {
    file: FileOrStdin,
    kind: ResolvedInputKind,
    fallback_kind: Option<ResolvedInputKind>,
}

pub enum ResolvedOutput {
    Count,
    Index,
}

impl ResolvedInput {
    fn run_engine<E: Engine>(self, engine: E, with_output: ResolvedOutput) -> Result<()> {
        match self.kind {
            ResolvedInputKind::Mmap => {
                let mmap_result = match &self.file {
                    FileOrStdin::File(f) => unsafe { MmapInput::map_file(f) },
                    FileOrStdin::Stdin(_) => todo!(),
                };

                match mmap_result {
                    Ok(input) => with_output.run_and_output(engine, input),
                    Err(err) => match self.fallback_kind {
                        Some(fallback_kind) => {
                            warn!(
                                "Creating a memory map failed: '{}'. Falling back to a slower input strategy.",
                                err
                            );
                            let new_input = ResolvedInput {
                                kind: fallback_kind,
                                fallback_kind: None,
                                file: self.file,
                            };

                            new_input.run_engine(engine, with_output)
                        }
                        None => Err(err).wrap_err("Creating a memory map failed."),
                    },
                }
            }
            ResolvedInputKind::Owned => {
                let contents = get_contents(self.file)?;
                let input = OwnedBytes::new(&contents)?;
                with_output.run_and_output(engine, input)
            }
            ResolvedInputKind::Buffered => {
                let input = BufferedInput::new(self.file);
                with_output.run_and_output(engine, input)
            }
        }
    }
}

impl ResolvedOutput {
    fn run_and_output<E: Engine, I: Input>(self, engine: E, input: I) -> Result<()> {
        fn run_impl<E: Engine, I: Input>(out: ResolvedOutput, engine: E, input: I) -> Result<(), EngineError> {
            match out {
                ResolvedOutput::Count => {
                    let result = engine.run::<_, CountResult>(&input)?;
                    print!("{result}");
                }
                ResolvedOutput::Index => {
                    let result = engine.run::<_, IndexResult>(&input)?;
                    print!("{result}");
                }
            }

            Ok(())
        }

        run_impl(self, engine, input).map_err(|err| report_engine_error(err).wrap_err("Error executing the query."))
    }
}

fn get_contents(mut file: FileOrStdin) -> Result<String> {
    let mut result = String::new();
    file.read_to_string(&mut result).wrap_err("Reading from file failed.")?;
    Ok(result)
}
