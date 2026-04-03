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

  private

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
