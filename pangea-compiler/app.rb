# frozen_string_literal: true

require "sinatra/base"
require "json"
require "terraform-synthesizer"

# Load all available pangea provider gems
%w[
  pangea-core
  pangea-aws
  pangea-akeyless
  pangea-cloudflare
  pangea-azure
  pangea-gcp
  pangea-hcloud
  pangea-kubernetes
  pangea-datadog
  pangea-splunk
].each do |gem_name|
  begin
    require gem_name.tr("-", "/")
  rescue LoadError => e
    $stderr.puts "Warning: #{gem_name} not available: #{e.message}"
  end
end

# Load architectures if available
begin
  require "pangea/architectures"
rescue LoadError => e
  $stderr.puts "Warning: pangea-architectures not available: #{e.message}"
end

# ---------------------------------------------------------------------------
# Generic Synthesizer Framework
#
# Two modes for two JSON shapes:
#
# 1. NestedSynthesizer (Terraform, CloudFormation) — method_missing maps to
#    nested hash keys via AbstractSynthesizer's `bury` pattern.
#    TerraformSynthesizer already handles this.
#
# 2. TypedArraySynthesizer — for tools whose JSON is arrays of typed objects
#    (Packer, Ansible playbooks, GitHub Actions). Each "section" is an array;
#    entries carry a `type` field derived from the DSL method name.
#
# Both share: synthesis → Hash, extend with provider modules, eval DSL source.
# ---------------------------------------------------------------------------

# Generic synthesizer for array-of-typed-objects JSON formats.
#
# Define sections (arrays) and map sections (hashes) at creation time:
#
#   synth = TypedArraySynthesizer.new(
#     array_sections: %w[builders provisioners post-processors],
#     map_sections:   %w[variables]
#   )
#
# The DSL then exposes one method per section (singularized):
#   synth.builder(:amazon_ebs, name: "nixos", ami_name: "...")
#   synth.provisioner(:shell, inline: ["cmd"])
#   synth.post_processor(:manifest, output: "manifest.json")
#   synth.variable(:source_ami, type: "string", default: "")
#
# For array sections, entries get a "type" field from the first argument.
# For map sections, entries are stored by name key.
#
class TypedArraySynthesizer
  attr_reader :manifest

  # section_config is a Hash mapping section names to their mode:
  #   { "builders" => :array, "variables" => :map }
  #
  # Or use the convenience keyword args:
  #   TypedArraySynthesizer.new(array_sections: [...], map_sections: [...])
  def initialize(array_sections: [], map_sections: [], section_config: nil)
    @section_config = section_config || {}
    array_sections.each { |s| @section_config[s.to_s] = :array }
    map_sections.each   { |s| @section_config[s.to_s] = :map }

    @manifest = {}
    @section_config.each do |name, mode|
      @manifest[name] = mode == :array ? [] : {}
    end

    # Build method→section lookup for singularized method names
    @method_map = {}
    @section_config.each_key do |section|
      singular = singularize(section)
      @method_map[singular] = section
      @method_map[section]  = section # also allow plural form
    end
  end

  def synthesis
    @manifest
  end

  # Dynamic DSL dispatch:
  #   synth.builder(:amazon_ebs, ami_name: "...", ...)
  #   synth.provisioner(:shell, inline: [...])
  #   synth.variable(:source_ami, type: "string")
  def method_missing(method_name, *args, **kwargs, &block)
    section = @method_map[method_name.to_s]
    return super unless section

    mode = @section_config[section]

    if mode == :array
      add_typed_entry(section, *args, **kwargs, &block)
    else
      add_map_entry(section, *args, **kwargs, &block)
    end
  end

  def respond_to_missing?(method_name, include_private = false)
    @method_map.key?(method_name.to_s) || super
  end

  private

  # Array section: first arg is the type, rest is config.
  # synth.builder(:amazon_ebs, ami_name: "...", instance_type: "c7i.xlarge")
  # synth.builder(:amazon_ebs, "nixos", ami_name: "...")  # optional name
  def add_typed_entry(section, type_name, name_or_nothing = nil, **config, &block)
    entry = { "type" => type_name.to_s.tr("_", "-") }

    # If second positional arg is a string/symbol, treat it as the entry name
    if name_or_nothing.is_a?(String) || name_or_nothing.is_a?(Symbol)
      entry["name"] = name_or_nothing.to_s
    elsif name_or_nothing.is_a?(Hash)
      config = name_or_nothing.merge(config)
    end

    entry.merge!(stringify_keys(config))
    yield entry if block
    @manifest[section] << entry
    entry
  end

  # Map section: first arg is the key name, kwargs are the value.
  # synth.variable(:source_ami, type: "string", default: "")
  def add_map_entry(section, key_name, **config, &block)
    value = stringify_keys(config)
    yield value if block
    @manifest[section][key_name.to_s] = value.empty? ? key_name.to_s : value
    value
  end

  def singularize(word)
    w = word.to_s
    if w.end_with?("ors")
      w.chomp("s")             # "post-processors" → "post-processor"
    elsif w.end_with?("ers")
      w.chomp("s")             # "builders" → "builder"
    elsif w.end_with?("s")
      w.chomp("s")             # "variables" → "variable"
    else
      w
    end
  end

  def stringify_keys(hash)
    hash.transform_keys { |k| k.to_s.tr("_", "-") }
  end
end

# ---------------------------------------------------------------------------
# Pre-built synthesizer factories
# ---------------------------------------------------------------------------

module SynthesizerRegistry
  REGISTRY = {}

  def self.register(name, &factory)
    REGISTRY[name.to_s] = factory
  end

  def self.create(name)
    factory = REGISTRY[name.to_s]
    raise "Unknown synthesizer: #{name}. Available: #{REGISTRY.keys.join(', ')}" unless factory

    factory.call
  end

  def self.available
    REGISTRY.keys
  end
end

# Packer: builders[], provisioners[], post-processors[], variables{}
SynthesizerRegistry.register(:packer) do
  TypedArraySynthesizer.new(
    array_sections: %w[builders provisioners post-processors],
    map_sections:   %w[variables]
  )
end

# Ansible playbook: plays[] with tasks[], handlers[], vars{}
SynthesizerRegistry.register(:ansible) do
  TypedArraySynthesizer.new(
    array_sections: %w[tasks handlers pre_tasks post_tasks],
    map_sections:   %w[vars]
  )
end

# GitHub Actions: jobs is a map, each with steps[]
SynthesizerRegistry.register(:github_actions) do
  TypedArraySynthesizer.new(
    array_sections: %w[steps],
    map_sections:   %w[env]
  )
end

# Convenience aliases
PackerSynthesizer = -> { SynthesizerRegistry.create(:packer) }

class PangeaCompiler < Sinatra::Base
  set :port, ENV.fetch("PORT", 8082)
  set :bind, "0.0.0.0"

  before do
    content_type :json
  end

  # Health check
  get "/healthz" do
    { status: "ok" }.to_json
  end

  # Compile a Pangea Ruby DSL template to Terraform JSON.
  #
  # Request body:
  #   {
  #     "source": "template :example do\n  ...\nend",
  #     "variables": { "cluster_name": "test" },
  #     "template_name": "example"  (optional, for multi-template files)
  #   }
  #
  # Response:
  #   {
  #     "terraform_json": "{ ... }",
  #     "template_count": 1,
  #     "errors": []
  #   }
  post "/compile" do
    begin
      body = JSON.parse(request.body.read)
      source = body["source"]
      variables = body["variables"] || {}
      template_name = body["template_name"]

      halt 400, { error: "Missing 'source' field" }.to_json unless source

      # Create a synthesizer context
      synth = TerraformSynthesizer.new

      # Extend with all available provider resources
      extend_synthesizer(synth)

      # Set variables in the binding context
      binding_context = create_binding(variables)

      # Evaluate the template source
      eval(source, binding_context, "(pangea-template)", 1) # rubocop:disable Security/Eval

      # Synthesize to Terraform JSON
      result = synth.synthesis

      {
        terraform_json: JSON.pretty_generate(result),
        template_count: 1,
        errors: []
      }.to_json
    rescue SyntaxError => e
      status 422
      { error: "Template syntax error: #{e.message}", errors: [e.message] }.to_json
    rescue StandardError => e
      status 422
      { error: "Compilation failed: #{e.message}", errors: [e.message, e.backtrace&.first(5)] }.to_json
    end
  end

  # Compile a Pangea Ruby DSL template to Packer JSON.
  # Convenience endpoint — delegates to the generic /compile-any with format=packer.
  post "/compile-packer" do
    compile_with_synthesizer(:packer, "packer_json")
  end

  # Generic compilation endpoint for any registered synthesizer format.
  #
  # Request body:
  #   {
  #     "format": "packer",           (or "ansible", "github_actions", etc.)
  #     "source": "synth.builder(:amazon_ebs, ...)",
  #     "variables": { "region": "us-east-1" }
  #   }
  #
  # Response:
  #   {
  #     "output_json": "{ ... }",
  #     "format": "packer",
  #     "errors": []
  #   }
  post "/compile-any" do
    begin
      body = JSON.parse(request.body.read)
      format = body["format"]

      halt 400, { error: "Missing 'format' field. Available: #{SynthesizerRegistry.available.join(', ')}" }.to_json unless format

      compile_with_synthesizer(format.to_sym, "output_json")
    rescue StandardError => e
      status 422
      { error: "Compilation failed: #{e.message}", errors: [e.message, e.backtrace&.first(5)] }.to_json
    end
  end

  # List available synthesizer formats.
  get "/formats" do
    { formats: SynthesizerRegistry.available }.to_json
  end

  private

  # Shared compilation logic for any synthesizer format.
  def compile_with_synthesizer(format, output_key)
    body = JSON.parse(request.body.read)
    source = body["source"]
    variables = body["variables"] || {}

    halt 400, { error: "Missing 'source' field" }.to_json unless source

    synth = SynthesizerRegistry.create(format)
    binding_context = create_binding(variables)

    # Make the synthesizer available as `synth` in the eval context
    binding_context.local_variable_set(:synth, synth)

    eval(source, binding_context, "(pangea-#{format}-template)", 1) # rubocop:disable Security/Eval

    result = synth.synthesis

    {
      output_key => JSON.pretty_generate(result),
      format: format.to_s,
      errors: []
    }.to_json
  rescue SyntaxError => e
    status 422
    { error: "#{format} template syntax error: #{e.message}", errors: [e.message] }.to_json
  rescue StandardError => e
    status 422
    { error: "#{format} compilation failed: #{e.message}", errors: [e.message, e.backtrace&.first(5)] }.to_json
  end

  def extend_synthesizer(synth)
    # Extend with available provider resource modules
    [
      Pangea::Resources::AWS,
      Pangea::Resources::Akeyless,
      Pangea::Resources::Cloudflare,
      Pangea::Resources::Azure,
      Pangea::Resources::GCP,
      Pangea::Resources::HCloud,
      Pangea::Resources::Kubernetes,
      Pangea::Resources::Datadog,
      Pangea::Resources::Splunk,
    ].each do |mod_const|
      synth.extend(mod_const)
    rescue NameError
      # Provider not loaded
    end
  end

  def create_binding(variables)
    b = binding
    variables.each do |key, value|
      b.local_variable_set(key.to_sym, value)
    end
    b
  end
end
