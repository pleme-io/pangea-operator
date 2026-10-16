mod builtins;
mod call;
mod core;
mod eval;
mod fmt;
mod interp;
mod json;
mod stdlib;
mod value;
mod yaml;

use std::path::{Path, PathBuf};
use std::rc::Rc;

pub use interp::Interp;

#[derive(Debug, thiserror::Error)]
pub enum RenderError {
    #[error("ruby raised {class}: {message}")]
    Raised { class: String, message: String },
    #[error("control flow escaped the template: {0}")]
    Escaped(&'static str),
}

#[derive(Clone, Debug, Default)]
pub struct RenderConfig {
    pub load_path: Vec<PathBuf>,
    pub env: Vec<(String, String)>,
    pub cwd: Option<PathBuf>,
}

pub struct Session {
    it: Rc<Interp>,
}

impl Session {
    pub fn new(cfg: &RenderConfig) -> Session {
        let it = Interp::new();
        it.load_path
            .borrow_mut()
            .extend(cfg.load_path.iter().cloned());
        for (k, v) in &cfg.env {
            it.env.borrow_mut().insert(k.clone(), v.clone());
        }
        if let Some(c) = &cfg.cwd {
            *it.cwd.borrow_mut() = c.clone();
        }
        Session { it }
    }

    pub fn eval(&self, src: &str, file: &str) -> Result<String, RenderError> {
        let top = self.it.top_frame.borrow().clone().unwrap();
        let sc = value::Scope::new(None);
        let r = self.it.eval_string(src, file, &top, &sc);
        self.finish(r)
    }

    fn finish(&self, r: interp::R) -> Result<String, RenderError> {
        match r {
            Ok(v) => Ok(core::to_s_plain(&v)),
            Err(interp::Ctrl::Raise(e)) => {
                let class = self.it.class_of(&e).display_name();
                let message = core::to_s_plain(&self.it.ivar_get(&e, "@message"));
                let bt = core::to_s_plain(&self.it.ivar_get(&e, "@__lava_bt"));
                let message = format!("{message}\n  {bt}");
                Err(RenderError::Raised { class, message })
            }
            Err(interp::Ctrl::Return(..)) => Err(RenderError::Escaped("return")),
            Err(interp::Ctrl::Break(..)) => Err(RenderError::Escaped("break")),
            Err(interp::Ctrl::Next(_)) => Err(RenderError::Escaped("next")),
            Err(interp::Ctrl::Throw(..)) => Err(RenderError::Escaped("throw")),
            Err(interp::Ctrl::Retry) => Err(RenderError::Escaped("retry")),
            Err(interp::Ctrl::StopIter(_)) => Err(RenderError::Escaped("StopIteration")),
        }
    }

    pub fn render_pangea_template(&self, template: &Path) -> Result<String, RenderError> {
        self.it.gvars.borrow_mut().insert(
            "$__lava_template".into(),
            value::V::str(template.display().to_string()),
        );
        self.eval(PANGEA_DRIVER, "/lava-ruby/driver.rb")
    }
}

const PANGEA_DRIVER: &str = r#"
require 'json'
require 'pangea/cli/config'
require 'pangea/cli/synthesizer'
config = Pangea::CLI::Config.new($__lava_template)
JSON.pretty_generate(Pangea::CLI::Synthesizer.new(config).synthesize)
"#;

#[derive(Clone, Debug, Default)]
pub struct OperatorCompile {
    pub source: Option<String>,
    pub template_path: Option<PathBuf>,
    pub variables: serde_json::Map<String, serde_json::Value>,
}

impl Session {
    pub fn compile_operator(
        &self,
        req: &OperatorCompile,
    ) -> Result<serde_json::Value, RenderError> {
        for (k, v) in &req.variables {
            let s = match v {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            self.it.env.borrow_mut().insert(k.clone(), s);
        }
        let vars = serde_json::Value::Object(req.variables.clone()).to_string();
        self.it
            .gvars
            .borrow_mut()
            .insert("$__lava_variables_json".into(), value::V::str(vars));
        let load_phase = match (&req.template_path, &req.source) {
            (Some(p), _) => {
                self.it.gvars.borrow_mut().insert(
                    "$__lava_template".into(),
                    value::V::str(p.display().to_string()),
                );
                "Dir.chdir(File.dirname($__lava_template)) do\n  load($__lava_template, true)\nend"
                    .to_string()
            }
            (None, Some(src)) => {
                self.it
                    .gvars
                    .borrow_mut()
                    .insert("$__lava_source".into(), value::V::str(src.clone()));
                "eval($__lava_source, TOPLEVEL_BINDING, \"(pangea-template)\", 1)".to_string()
            }
            (None, None) => {
                return Err(RenderError::Raised {
                    class: "ArgumentError".into(),
                    message: "compile request needs either source or template_path".into(),
                })
            }
        };
        let driver = OPERATOR_DRIVER.replace("{load_phase}", &load_phase);
        let out = self.eval(&driver, "/lava-ruby/operator-driver.rb")?;
        serde_json::from_str(&out).map_err(|e| RenderError::Raised {
            class: "JSON::ParserError".into(),
            message: e.to_string(),
        })
    }
}

const OPERATOR_DRIVER: &str = r#"
require 'json'
$pangea_variables = JSON.parse($__lava_variables_json)
$pangea_captured_block = nil
begin
  Object.send(:define_method, :template) do |_name, &blk|
    $pangea_captured_block = blk
  end
  {load_phase}
  if $pangea_captured_block.nil?
    raise "no template :name do … end block found"
  end
  synth =
    if defined?(TerraformSynthesizer)
      s = TerraformSynthesizer.new
      if defined?(Pangea::Resources)
        Pangea::Resources.constants.each do |c|
          m = Pangea::Resources.const_get(c)
          s.extend(m) if m.is_a?(Module) && !m.is_a?(Class)
        end
      end
      s
    else
      raise "TerraformSynthesizer not loaded"
    end
  synth.instance_eval(&$pangea_captured_block)
  out = synth.synthesis
  Pangea::ProviderRegistry.inject_into_synthesis(out) if defined?(Pangea::ProviderRegistry)
  JSON.generate(out)
ensure
  if Object.method_defined?(:template, true) || Object.private_method_defined?(:template, true)
    Object.send(:remove_method, :template) rescue nil
  end
  $pangea_captured_block = nil
end
"#;

pub fn compile_operator(
    req: &OperatorCompile,
    cfg: &RenderConfig,
) -> Result<serde_json::Value, RenderError> {
    Session::new(cfg).compile_operator(req)
}

pub fn render_pangea_template(template: &Path, cfg: &RenderConfig) -> Result<String, RenderError> {
    Session::new(cfg).render_pangea_template(template)
}

pub fn eval_ruby(src: &str, cfg: &RenderConfig) -> Result<String, RenderError> {
    Session::new(cfg).eval(src, "(lava-ruby)")
}
