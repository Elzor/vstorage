#[macro_use]
extern crate clap;

use clap::{crate_authors, crate_version, crate_name, App, Arg};

extern crate dirs;

use std::borrow::Cow::{self, Owned};
use std::fs;
use std::io::Error as IoError;
use std::process;

use rustyline::{ColorMode, CompletionType, Config, Context, EditMode, Editor, Helper};
use rustyline::completion::{Completer, FilenameCompleter, Pair};
use rustyline::config::OutputStreamType;
use rustyline::error::ReadlineError;
use rustyline::highlight::{Highlighter, MatchingBracketHighlighter};
use rustyline::hint::{Hinter, HistoryHinter};
use vstorage::binutil;

use block_api::block_api_client::BlockApiClient;
use block_api::{IdxRequest, StatusRequest};

pub mod block_api {
    tonic::include_proto!("block_api");
}

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
        "{}-ctl / v{}\n\nVshell (abort with ^C or ^D)",
        crate_name!(),
        crate_version!()
    )
}

#[tokio::main]
async fn main() {
    if let Err(e) = init() {
        panic!("init phase failed: {}", e.to_string());
    }

    show_welcome();

    let default_block_server_config = vstorage::config::Config::default();
    let matches = App::new(APP_NAME)
        .about("Storage common CLI. Powered by Vonmo")
        .author(crate_authors!())
        .version(crate_version!())
        .long_version(binutil::vm_version_info().as_ref())
        .arg(
            Arg::with_name("host")
                .short("h")
                .long("host")
                .help("block server endpoint")
                .default_value(default_block_server_config.interfaces.grpc_internal.as_str())
                .takes_value(true),
        )
        .get_matches();

    let endpoint = matches
        .value_of("host")
        .ok_or(default_block_server_config.interfaces.grpc_internal.as_str())
        .unwrap().to_string();

    let mut client = match BlockApiClient::connect(format!("http://{}", endpoint)).await {
        Ok(client) => client,
        Err(e) => {
            println!("connection error: {}", e);
            process::exit(1)
        }
    };
    println!("connected to {}", endpoint);

    match client.idx(tonic::Request::new(IdxRequest {})).await {
        Ok(res) => {
            if res.into_inner().message.eq("") {
                println!("init error: wrong idx response");
                process::exit(1)
            }
        }
        Err(e) => {
            println!("init error: {}", e);
            process::exit(1)
        }
    }

    let cli_matches = match client.status(tonic::Request::new(StatusRequest {})).await {
        Ok(res) => {
            let role =  res.into_inner().node.unwrap().role;
            if role.eq("") {
                println!("init error: wrong node.role");
                process::exit(1)
            }
            match role.to_lowercase().as_str() {
                "storage" => {
                    clap_app!(storage =>
                        (version: "1.0")
                        (about: "Storage CLI")
                        (@arg debug: -d ... "Sets the level of debugging information")
                        (@subcommand status =>
                            (version: "1.0")
                            (about: "Shows all information about storage node")
                        )
                        (@subcommand uptime =>
                            (version: "1.0")
                            (about: "Prints host and node uptime")
                            (@arg node: -n --node "Print node uptime")
                            (@arg host: -h --host "Print host uptime")
                        )
                    )
                }
                "meta" => {
                    clap_app!(meta =>
                        (version: "1.0")
                        (about: "meta cli")
                        (@arg debug: -d ... "Sets the level of debugging information")
                        (@subcommand uptime =>
                            (about: "host and node uptime")
                            (@arg node: -n --node "Print node uptime")
                            (@arg host: -h --host "Print host uptime")
                        )
                    )
                }
                "gate" => {
                    clap_app!(meta =>
                        (version: "1.0")
                        (about: "gateway cli")
                        (@arg debug: -d ... "Sets the level of debugging information")
                        (@subcommand uptime =>
                            (about: "host and node uptime")
                            (@arg node: -n --node "Print node uptime")
                            (@arg host: -h --host "Print host uptime")
                        )
                    )
                }
                other => {
                    println!("error: unknown node.role {}", other);
                    process::exit(1)
                }
            }
        }
        Err(e) => {
            println!("init error: {}", e);
            process::exit(1)
        }
    };

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
                "exit" | "quit" | "q()." | "disconnect" | "\\q" => {
                    println!("bye!");
                    process::exit(0);
                }
                _ => {
                    editor.add_history_entry(line.to_string());
                    if line.to_string().trim().eq("") {
                        continue;
                    }
                    let cli_cmds = cli_matches.clone();
                    line = line.to_string().to_lowercase().trim().to_string();
                    let args: Vec<&str> = line.split(" ").collect();
                    let cmd = cli_cmds.get_matches_from_safe(args);
                    match cmd {
                        Ok(cmd) => {
                            match cmd.subcommand {
                                Some(subcmd) => {
                                    dbg!(subcmd);
                                }
                                _ => {
                                    println!("unknown command")
                                }
                            }
                        }
                        Err(why) => {
                            match why.kind {
                                clap::ErrorKind::HelpDisplayed
                                | clap::ErrorKind::VersionDisplayed
                                | clap::ErrorKind::UnknownArgument => {
                                    println!("{}", why.message);
                                }
                                _any => {
                                    dbg!(&why);
                                    println!("{}", why.message);
                                }
                            }
                        }
                    }
                }
            }
            break;
        }
        editor.save_history(&history_file()).unwrap();
        count += 1
    }
}
