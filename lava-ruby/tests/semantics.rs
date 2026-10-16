use lava_ruby::{eval_ruby, RenderConfig};

fn rb(src: &str) -> String {
    eval_ruby(src, &RenderConfig::default()).unwrap_or_else(|e| panic!("{src}\n=> {e}"))
}

fn rb_err(src: &str) -> String {
    eval_ruby(src, &RenderConfig::default())
        .expect_err(src)
        .to_string()
}

#[test]
fn literals_and_inspect() {
    assert_eq!(
        rb(r#"{a: 1, "b" => [2.5, nil, :c]}.inspect"#),
        r#"{:a=>1, "b"=>[2.5, nil, :c]}"#
    );
    assert_eq!(rb(r#"x = 3; "v=#{x * 2}""#), "v=6");
    assert_eq!(rb("(1..4).map { |i| i ** 2 }.sum.to_s"), "30");
    assert_eq!(rb("[3, 1, 2].sort_by { |x| -x }.inspect"), "[3, 2, 1]");
    assert_eq!(rb("1e20.to_s"), "1.0e+20");
    assert_eq!(rb("(-7 / 2).to_s"), "-4");
}

#[test]
fn classes_modules_and_dispatch() {
    let src = r#"
      module Greeter
        def self.included(base); base.extend(ClassMethods); end
        module ClassMethods; def tag; "cm"; end; end
        def greet; "hi #{name}"; end
      end
      class Person
        include Greeter
        attr_reader :name
        def initialize(name:); @name = name; end
        def method_missing(m, *args); m.to_s.start_with?("x_") ? m.to_s : super; end
        def respond_to_missing?(m, p = false); m.to_s.start_with?("x_") || super; end
      end
      p = Person.new(name: "ana")
      [p.greet, Person.tag, p.x_y, p.respond_to?(:x_z), p.respond_to?(:q)].inspect
    "#;
    assert_eq!(rb(src), r#"["hi ana", "cm", "x_y", true, false]"#);
}

#[test]
fn blocks_instance_eval_and_control_flow() {
    let src = r#"
      class Collector
        def initialize; @d = {}; end
        def method_missing(n, *a, &b)
          if b then s = Collector.new; s.instance_eval(&b); @d[n] = s.to_h else @d[n] = a[0] end
        end
        def to_h; @d; end
      end
      c = Collector.new
      outer = "o"
      c.instance_eval { provider { owner outer; token "t" }; count 2 }
      def first_even(xs); xs.each { |x| return x if x.even? }; nil; end
      [c.to_h, first_even([1, 3, 4, 6]), [1, 2, 3].each_with_object([]) { |x, acc| next if x == 2; acc << x }].inspect
    "#;
    assert_eq!(
        rb(src),
        r#"[{:provider=>{:owner=>"o", :token=>"t"}, :count=>2}, 4, [1, 3]]"#
    );
}

#[test]
fn exceptions_rescue_ensure() {
    let src = r#"
      log = []
      begin
        begin
          raise ArgumentError, "bad"
        ensure
          log << :ensure
        end
      rescue KeyError
        log << :wrong
      rescue StandardError => e
        log << e.class.name << e.message
      end
      log.inspect
    "#;
    assert_eq!(rb(src), r#"[:ensure, "ArgumentError", "bad"]"#);
    assert!(rb_err("raise 'boom'").contains("RuntimeError"));
}

#[test]
fn dry_struct_and_types() {
    let src = r#"
      module T; include Dry.Types(); end
      class Attrs < Dry::Struct
        transform_keys(&:to_sym)
        attribute :name, T::String
        attribute? :tags, T::Array.of(T::String).optional
        attribute? :mode, T::Coercible::Symbol.default(:auto).enum(:auto, :none)
      end
      a = Attrs.new("name" => "x", tags: ["a"])
      bad = begin; Attrs.new(name: 1); rescue Dry::Struct::Error => e; e.class.name; end
      [a.to_h, a.mode, Attrs.schema.map(&:name), bad, Attrs.load({name: "y"}).to_h].inspect
    "#;
    assert_eq!(
        rb(src),
        r#"[{:name=>"x", :tags=>["a"], :mode=>:auto}, :auto, [:name, :tags, :mode], "Dry::Struct::Error", {:name=>"y"}]"#
    );
}

#[test]
fn json_pretty_matches_json_2_7() {
    assert_eq!(
        rb(r#"JSON.pretty_generate({"a" => [], "b" => {"c" => [1]}, d: "—"})"#),
        "{\n  \"a\": [\n\n  ],\n  \"b\": {\n    \"c\": [\n      1\n    ]\n  },\n  \"d\": \"—\"\n}"
    );
    assert_eq!(
        rb(r#"JSON.generate(JSON.parse('{"x":[1,2.5,null,true]}'))"#),
        r#"{"x":[1,2.5,null,true]}"#
    );
}

#[test]
fn yaml_uses_psych_1_1_scalars() {
    assert_eq!(
        rb(
            r#"YAML.safe_load("a: yes\nb: 'no'\nc: :sym\nd: 0x1f\ne: ~\nf: 1_000\n", permitted_classes: [Symbol]).inspect"#
        ),
        r#"{"a"=>true, "b"=>"no", "c"=>:sym, "d"=>31, "e"=>nil, "f"=>1000}"#
    );
}

#[test]
fn nondeterminism_is_refused() {
    assert!(rb_err("Time.now").contains("nondeterministic"));
    assert!(rb_err("`ls`").contains("not available"));
    assert!(rb_err("system('true')").contains("not available"));
}

#[test]
fn env_is_explicit_not_inherited() {
    let cfg = RenderConfig {
        env: vec![("ONLY".into(), "1".into())],
        ..Default::default()
    };
    assert_eq!(
        eval_ruby(
            "[ENV['ONLY'], ENV['HOME'], ENV.fetch('PATH', 'unset')].inspect",
            &cfg
        )
        .unwrap(),
        r#"["1", nil, "unset"]"#
    );
}
