# frozen_string_literal: true
#
# Pangea Compiler — packaged as a real Ruby gem so the standard nixpkgs
# bundlerApp pattern (`pname`/`gemdir`/`exes`) works without ceremony.
# Runtime deps live here; the Gemfile just says `gemspec` and adds the
# vendored provider gems via path:.

Gem::Specification.new do |spec|
  spec.name        = 'pangea-compiler'
  spec.version     = '0.1.0'
  spec.authors     = ['Pangea Team']
  spec.email       = ['ops@pleme.io']
  spec.summary     = 'Pangea DSL → Terraform JSON HTTP sidecar'
  spec.description = <<~DESC
    Sinatra service that the Pangea operator calls to compile
    InfrastructureTemplate Ruby DSL into Terraform JSON. Loads every
    available pangea-* provider gem at boot.
  DESC
  spec.license     = 'Apache-2.0'
  spec.required_ruby_version = '>= 3.3.0'

  spec.files = Dir[
    'app.rb',
    'config.ru',
    'bin/pangea-compiler',
    'lib/**/*.rb',
    'pangea-compiler.gemspec',
    'Gemfile',
    'Gemfile.lock',
  ].select { |f| File.file?(f) }

  spec.bindir      = 'bin'
  spec.executables = ['pangea-compiler']

  # HTTP layer
  spec.add_dependency 'sinatra', '~> 4.0'
  spec.add_dependency 'puma',    '~> 6.0'
  spec.add_dependency 'json'

  # Synthesis framework
  spec.add_dependency 'terraform-synthesizer', '~> 0.0.28'

  # Provider gems — declared with `~> 0` so any version satisfies; the
  # actual versions are pinned by Gemfile.lock against the path: entries
  # in the Gemfile.
  %w[
    pangea-core
    pangea-akeyless
    pangea-aws
    pangea-azure
    pangea-cloudflare
    pangea-datadog
    pangea-gcp
    pangea-hcloud
    pangea-kubernetes
    pangea-porkbun
    pangea-splunk
    pangea-spot
    pangea-architectures
  ].each { |g| spec.add_dependency g }

  spec.metadata['rubygems_mfa_required'] = 'true'
end
