extern crate clap;
extern crate dirs;

use clap::{crate_name, crate_version};

use std::borrow::Cow::{self, Borrowed, Owned};
use std::fs;
use std::io::Error as IoError;
use std::process;

use rustyline::completion::{Completer, FilenameCompleter, Pair};
use rustyline::config::OutputStreamType;
use rustyline::error::ReadlineError;
use rustyline::highlight::{Highlighter, MatchingBracketHighlighter};
use rustyline::hint::{Hinter, HistoryHinter};
use rustyline::{ColorMode, CompletionType, Config, Context, EditMode, Editor, Helper};

const APP_NAME: &str = "block_ctl";

fn home_dir() -> String {
    match dirs::config_dir() {
        Some(path) => path.display().to_string(),
        None => {
            println!("WRN: Impossible to get your home dir!");
            "".to_string()
        }
    }
}

fn app_dir() -> String {
    home_dir() + "/" + APP_NAME
}

fn history_file() -> String {
    app_dir() + "/history.txt"
}

fn init() -> Result<(), IoError> {
    fs::create_dir_all(app_dir())?;
    Ok(())
}

struct CtlHelper {
    completer: FilenameCompleter,
    highlighter: MatchingBracketHighlighter,
    hinter: HistoryHinter,
    colored_prompt: String,
}

impl Completer for CtlHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        ctx: &Context<'_>,
    ) -> Result<(usize, Vec<Pair>), ReadlineError> {
        self.completer.complete(line, pos, ctx)
    }
}

impl Hinter for CtlHelper {
    fn hint(&self, line: &str, pos: usize, ctx: &Context<'_>) -> Option<String> {
        self.hinter.hint(line, pos, ctx)
    }
}

impl Highlighter for CtlHelper {
    fn highlight<'l>(&self, line: &'l str, pos: usize) -> Cow<'l, str> {
        self.highlighter.highlight(line, pos)
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        Owned("\x1b[1m".to_owned() + hint + "\x1b[m")
    }

    fn highlight_char(&self, line: &str, pos: usize) -> bool {
        self.highlighter.highlight_char(line, pos)
    }
}

impl Helper for CtlHelper {}

fn show_welcome() {
    println!(
        "{} / v{}\n\nVshell (abort with ^C or ^D)",
        crate_name!(),
        crate_version!()
    )
}

fn main() {
    if let Err(e) = init() {
        panic!("init phase failed: {}", e.to_string());
    }

    show_welcome();

    let config = Config::builder()
        .history_ignore_space(true)
        .completion_type(CompletionType::List)
        .edit_mode(EditMode::Vi)
        .output_stream(OutputStreamType::Stdout)
        .color_mode(ColorMode::Enabled)
        .build();

    let helper = CtlHelper {
        completer: FilenameCompleter::new(),
        highlighter: MatchingBracketHighlighter::new(),
        hinter: HistoryHinter {},
        colored_prompt: "".to_owned(),
    };

    let mut editor = Editor::with_config(config);
    editor.set_helper(Some(helper));

    if editor.load_history(&history_file()).is_err() {
        println!("No previous history.");
    }

    let mut count = 1;
    loop {
        let prompt = format!("{}> ", count);
        editor.helper_mut().unwrap().colored_prompt = format!("\x1b[1;32m{}\x1b[0m", prompt);
        let mut line = String::new();
        loop {
            match editor.readline(&prompt) {
                Ok(input) => line.push_str(&input),
                Err(ReadlineError::Interrupted) => {
                    println!("CTRL-C");
                    return;
                }
                Err(ReadlineError::Eof) => {
                    println!("CTRL-D");
                    return;
                }
                Err(err) => {
                    println!("ERR: {:?}", err);
                    break;
                }
            }
            match line.to_string().to_lowercase().trim() {
                "exit" | "quit" | "q()." => {
                    println!("bye!");
                    process::exit(0);
                }
                _ => {
                    editor.add_history_entry(line.to_string());
                    if line.to_string().trim().eq("") {
                        continue;
                    }
                    println!("line: {}", line.to_string());
                }
            }
            break;
        }
        editor.save_history(&history_file()).unwrap();
        count += 1
    }
}
