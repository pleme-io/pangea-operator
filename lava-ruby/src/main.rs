use std::path::PathBuf;
use std::process::ExitCode;

use lava_ruby::{
    compile_operator, eval_ruby, render_pangea_template, OperatorCompile, RenderConfig,
};

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let mut cfg = RenderConfig::default();
    let mut mode = None;
    let mut target = None;
    while let Some(a) = args.next() {
        match a.as_str() {
            "-I" => cfg
                .load_path
                .push(PathBuf::from(args.next().unwrap_or_default())),
            "--env" => {
                let kv = args.next().unwrap_or_default();
                if let Some((k, v)) = kv.split_once('=') {
                    cfg.env.push((k.to_string(), v.to_string()));
                }
            }
            "-e" => {
                mode = Some("eval");
                target = args.next();
            }
            "render" => mode = Some("render"),
            "compile" => mode = Some("compile"),
            other => target = Some(other.to_string()),
        }
    }
    let r = match (mode, target) {
        (Some("eval"), Some(src)) => eval_ruby(&src, &cfg),
        (Some("render"), Some(t)) => render_pangea_template(&PathBuf::from(t), &cfg),
        (Some("compile"), Some(t)) => {
            let req = OperatorCompile {
                template_path: Some(PathBuf::from(t)),
                ..Default::default()
            };
            compile_operator(&req, &cfg).map(|v| v.to_string())
        }
        _ => {
            eprintln!("usage: lava-ruby [-I dir]... [--env K=V]... (render TEMPLATE.rb | -e SRC)");
            return ExitCode::from(2);
        }
    };
    match r {
        Ok(s) => {
            print!("{s}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
