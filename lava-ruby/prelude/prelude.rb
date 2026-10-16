class Set
  include Enumerable

  def self.[](*items)
    new(items)
  end

  def initialize(items = nil, &block)
    @hash = {}
    return if items.nil?
    items.each { |x| @hash[block ? block.call(x) : x] = true }
  end

  def add(x)
    @hash[x] = true
    self
  end
  alias << add

  def add?(x)
    return nil if @hash.key?(x)
    @hash[x] = true
    self
  end

  def merge(*enums)
    enums.each { |e| e.each { |x| @hash[x] = true } }
    self
  end

  def delete(x)
    @hash.delete(x)
    self
  end

  def delete?(x)
    return nil unless @hash.key?(x)
    @hash.delete(x)
    self
  end

  def include?(x)
    @hash.key?(x)
  end
  alias member? include?
  alias === include?
  alias contain? include?

  def each(&block)
    return to_a unless block
    @hash.keys.each(&block)
    self
  end

  def size
    @hash.size
  end
  alias length size

  def count(*args, &block)
    return @hash.size if args.empty? && block.nil?
    to_a.count(*args, &block)
  end

  def empty?
    @hash.empty?
  end

  def any?(*args, &block)
    to_a.any?(*args, &block)
  end

  def to_a
    @hash.keys
  end
  alias entries to_a

  def to_set
    self
  end

  def dup
    Set.new(to_a)
  end
  alias clone dup

  def |(other)
    Set.new(to_a + other.to_a)
  end
  alias union |
  alias + |

  def &(other)
    o = other.is_a?(Set) ? other : Set.new(other)
    Set.new(to_a.select { |x| o.include?(x) })
  end
  alias intersection &

  def -(other)
    o = other.is_a?(Set) ? other : Set.new(other)
    Set.new(to_a.reject { |x| o.include?(x) })
  end
  alias difference -

  def ^(other)
    (self | other) - (self & other)
  end

  def subset?(other)
    all? { |x| other.include?(x) }
  end
  alias <= subset?

  def superset?(other)
    other.all? { |x| include?(x) }
  end
  alias >= superset?

  def proper_subset?(other)
    subset?(other) && size < other.size
  end
  alias < proper_subset?

  def disjoint?(other)
    (self & other).empty?
  end

  def intersect?(other)
    !disjoint?(other)
  end

  def ==(other)
    other.is_a?(Set) && size == other.size && subset?(other)
  end
  alias eql? ==

  def hash
    to_a.map(&:hash).sort.hash
  end

  def freeze
    @hash.freeze
    super
  end

  def select!(&block)
    @hash.keys.each { |k| @hash.delete(k) unless block.call(k) }
    self
  end
  alias filter! select!

  def reject!(&block)
    @hash.keys.each { |k| @hash.delete(k) if block.call(k) }
    self
  end
  alias delete_if reject!

  def clear
    @hash.clear
    self
  end

  def map!(&block)
    items = to_a.map(&block)
    @hash = {}
    items.each { |x| @hash[x] = true }
    self
  end

  def to_s
    inspect
  end

  def inspect
    "#<Set: {#{to_a.map(&:inspect).join(', ')}}>"
  end

  def sort(&block)
    to_a.sort(&block)
  end

  def min(*a, &b)
    to_a.min(*a, &b)
  end

  def max(*a, &b)
    to_a.max(*a, &b)
  end
end

class Date
  include Comparable
  class Error < ArgumentError; end
  attr_reader :year, :month, :day

  def self.civil_to_jd(y, m, d)
    a = (14 - m) / 12
    yy = y + 4800 - a
    mm = m + 12 * a - 3
    d + (153 * mm + 2) / 5 + 365 * yy + yy / 4 - yy / 100 + yy / 400 - 32045
  end

  def self.jd_to_civil(jd)
    a = jd + 32044
    b = (4 * a + 3) / 146097
    c = a - 146097 * b / 4
    d = (4 * c + 3) / 1461
    e = c - 1461 * d / 4
    m = (5 * e + 2) / 153
    [100 * b + d - 4800 + m / 10, m + 3 - 12 * (m / 10), e - (153 * m + 2) / 5 + 1]
  end

  def self.parse(str)
    m = str.to_s.match(/(\d{4})-(\d{1,2})-(\d{1,2})/)
    raise Error, 'invalid date' unless m
    new(m[1].to_i, m[2].to_i, m[3].to_i)
  end

  def self.iso8601(str)
    parse(str)
  end

  def self.today
    parse(__lava_today)
  end

  def initialize(year = -4712, month = 1, day = 1)
    @year = year
    @month = month
    @day = day
  end

  def jd
    Date.civil_to_jd(@year, @month, @day)
  end

  def +(n)
    y, m, d = Date.jd_to_civil(jd + n.to_i)
    Date.new(y, m, d)
  end

  def -(other)
    other.is_a?(Date) ? jd - other.jd : self + -other
  end

  def <=>(other)
    other.is_a?(Date) ? jd <=> other.jd : nil
  end

  def ==(other)
    other.is_a?(Date) && jd == other.jd
  end

  def to_s
    format('%04d-%02d-%02d', @year, @month, @day)
  end
  alias iso8601 to_s

  def to_date
    self
  end

  def inspect
    "#<Date: #{self}>"
  end
end

module Socket
  AF_INET = 2
  AF_INET6 = 30
end

class IPAddr
  class InvalidAddressError < ArgumentError; end
  include Comparable
  attr_reader :prefix

  def initialize(addr = '0.0.0.0', family = nil)
    if addr.is_a?(Integer)
      @value = addr
      @prefix = 32
      return
    end
    ip, len = addr.to_s.split('/', 2)
    octets = ip.split('.')
    unless octets.size == 4 && octets.all? { |o| o.match?(/\A\d{1,3}\z/) && o.to_i <= 255 }
      raise InvalidAddressError, "invalid address: #{addr}"
    end
    @value = octets.inject(0) { |acc, o| (acc << 8) | o.to_i }
    @prefix = len ? Integer(len) : 32
    raise InvalidAddressError, "invalid length: #{addr}" if @prefix.negative? || @prefix > 32
    @value &= mask_value
  end

  def mask_value
    @prefix.zero? ? 0 : ((0xffffffff << (32 - @prefix)) & 0xffffffff)
  end

  def to_i
    @value
  end

  def to_s
    [24, 16, 8, 0].map { |s| (@value >> s) & 255 }.join('.')
  end

  def to_string
    to_s
  end

  def include?(other)
    o = other.is_a?(IPAddr) ? other : IPAddr.new(other.to_s)
    (o.to_i & mask_value) == @value
  end
  alias === include?

  def <=>(other)
    to_i <=> other.to_i
  end

  def ipv4?
    true
  end

  def family
    Socket::AF_INET
  end

  def ipv6?
    false
  end

  def private?
    IPAddr.new('10.0.0.0/8').include?(self) || IPAddr.new('172.16.0.0/12').include?(self) || IPAddr.new('192.168.0.0/16').include?(self)
  end

  def mask(len)
    IPAddr.new("#{self}/#{len}")
  end

  def succ
    n = IPAddr.new(to_s)
    n.instance_variable_set(:@value, @value + 1)
    n
  end

  def inspect
    "#<IPAddr: IPv4:#{self}/#{@prefix}>"
  end
end

class Mutex
  def synchronize
    yield
  end

  def lock
    self
  end

  def unlock
    self
  end

  def locked?
    false
  end

  def owned?
    false
  end
end

class Monitor < Mutex
  def mon_synchronize
    yield
  end
end

module Singleton
  def self.included(base)
    base.extend(ClassMethods)
  end

  module ClassMethods
    def instance
      @singleton__instance__ ||= new
    end
  end
end

module Forwardable
  def def_delegators(accessor, *methods)
    methods.each { |m| def_delegator(accessor, m) }
  end

  def def_delegator(accessor, method, ali = method)
    acc = accessor.to_s
    define_method(ali) do |*args, **kw, &block|
      target = acc.start_with?('@') ? instance_variable_get(acc) : send(acc)
      target.send(method, *args, **kw, &block)
    end
  end
  alias delegate def_delegators
end

class OpenStruct
  def initialize(hash = nil)
    @table = {}
    (hash || {}).each { |k, v| @table[k.to_sym] = v }
  end

  def [](k)
    @table[k.to_sym]
  end

  def []=(k, v)
    @table[k.to_sym] = v
  end

  def to_h
    @table.dup
  end

  def respond_to_missing?(name, include_private = false)
    @table.key?(name.to_s.chomp('=').to_sym) || super
  end

  def method_missing(name, *args)
    n = name.to_s
    if n.end_with?('=')
      @table[n.chomp('=').to_sym] = args[0]
    else
      @table[name]
    end
  end

  def dig(*keys)
    @table.dig(*keys)
  end
end

class Struct
  def self.new(*fields, keyword_init: nil, &block)
    klass = Class.new(Struct::Base)
    klass.instance_variable_set(:@__fields, fields.map(&:to_sym))
    klass.instance_variable_set(:@__kw, keyword_init)
    fields.each do |f|
      klass.define_method(f) { @__values[f.to_sym] }
      klass.define_method(:"#{f}=") { |v| @__values[f.to_sym] = v }
    end
    klass.class_eval(&block) if block
    klass
  end

  class Base
    include Enumerable

    def self.members
      @__fields || superclass.members
    end

    def self.[](*args)
      new(*args)
    end

    def initialize(*args, **kw)
      @__values = {}
      fields = self.class.members
      if !kw.empty? && args.empty?
        kw.each do |k, v|
          raise ArgumentError, "unknown keywords: #{k}" unless fields.include?(k.to_sym)
          @__values[k.to_sym] = v
        end
      else
        args << kw unless kw.empty?
        raise ArgumentError, 'struct size differs' if args.size > fields.size
        fields.each_with_index { |f, i| @__values[f] = args[i] }
      end
      fields.each { |f| @__values[f] = nil unless @__values.key?(f) }
    end

    def members
      self.class.members
    end

    def [](k)
      k.is_a?(Integer) ? @__values[members[k]] : @__values[k.to_sym]
    end

    def []=(k, v)
      @__values[k.is_a?(Integer) ? members[k] : k.to_sym] = v
    end

    def to_h(&block)
      h = @__values.dup
      block ? h.to_h(&block) : h
    end
    alias to_hash to_h

    def to_a
      members.map { |m| @__values[m] }
    end
    alias deconstruct to_a

    def each(&block)
      to_a.each(&block)
    end

    def ==(other)
      other.class == self.class && other.to_h == to_h
    end

    def hash
      to_a.hash
    end

    def dig(*keys)
      to_h.dig(*keys)
    end

    def inspect
      "#<struct #{self.class.name} #{members.map { |m| "#{m}=#{@__values[m].inspect}" }.join(', ')}>"
    end
    alias to_s inspect
  end
end

class Pathname
  def initialize(path)
    @path = path.to_s
  end

  def to_s
    @path
  end
  alias to_path to_s

  def join(*parts)
    Pathname.new(File.join(@path, *parts.map(&:to_s)))
  end

  def +(other)
    join(other)
  end
  alias / +

  def dirname
    Pathname.new(File.dirname(@path))
  end

  def basename(*a)
    Pathname.new(File.basename(@path, *a))
  end

  def expand_path(*a)
    Pathname.new(File.expand_path(@path, *a))
  end

  def exist?
    File.exist?(@path)
  end

  def file?
    File.file?(@path)
  end

  def directory?
    File.directory?(@path)
  end

  def read
    File.read(@path)
  end

  def extname
    File.extname(@path)
  end

  def ==(o)
    o.to_s == @path
  end
end

module Kernel
  def Pathname(p)
    Pathname.new(p)
  end
end

module Dry
  module Core
    module Constants
      EMPTY_HASH = {}.freeze
      EMPTY_ARRAY = [].freeze
      Undefined = Object.new
      def Undefined.inspect
        'Undefined'
      end

      def Undefined.to_s
        'Undefined'
      end

      def Undefined.default(x, y = nil)
        equal?(self) ? (block_given? ? yield : x) : self
      end
    end
  end

  module Types
    Undefined = Dry::Core::Constants::Undefined

    class CoercionError < StandardError
      def initialize(message = nil, meta: nil)
        super(message)
      end
    end

    class ConstraintError < CoercionError
      attr_reader :result, :input

      def initialize(result, input = nil)
        @result = result
        @input = input
        if result.is_a?(String)
          super(result)
        else
          super("#{input.inspect} violates constraints (#{result} failed)")
        end
      end
    end

    class SchemaError < CoercionError
      def initialize(key, value = nil, result = nil)
        super("#{value.inspect} (#{value.class}) has invalid type for :#{key} violates constraints (#{result})")
      end
    end

    class MissingKeyError < SchemaError
      attr_reader :key

      def initialize(key)
        @key = key
        StandardError.instance_method(:initialize)
        @message = "#{key.inspect} is missing in Hash input"
      end

      def message
        @message
      end
    end

    class UnknownKeysError < SchemaError
      def initialize(keys)
        @message = "unexpected keys #{keys.inspect} in Hash input"
      end

      def message
        @message
      end
    end

    class MapError < CoercionError; end

    class Result
      class Success
        attr_reader :input

        def initialize(input)
          @input = input
        end

        def success?
          true
        end

        def failure?
          false
        end
      end

      class Failure
        attr_reader :input, :error

        def initialize(input, error)
          @input = input
          @error = error
        end

        def success?
          false
        end

        def failure?
          true
        end
      end
    end

    module Builder
      def call(input = Undefined, &block)
        if block
          begin
            call_unsafe(input)
          rescue CoercionError
            yield(input)
          end
        else
          call_unsafe(input)
        end
      end

      def [](input = Undefined)
        call(input)
      end

      def call_safe(input)
        call_unsafe(input)
      rescue CoercionError
        block_given? ? yield(input) : input
      end

      def try(input)
        Result::Success.new(call_unsafe(input))
      rescue CoercionError => e
        f = Result::Failure.new(input, e)
        block_given? ? yield(f) : f
      end

      def valid?(input)
        call_unsafe(input)
        true
      rescue CoercionError
        false
      end
      alias === valid?

      def optional
        Sum.new(Strict.nominal(NilClass), self)
      end

      def maybe
        optional
      end

      def default(value = Undefined, &block)
        Default.new(self, block || value)
      end

      def constrained(**rules)
        Constrained.new(self, rules)
      end

      def enum(*values)
        values = values.first.keys if values.size == 1 && values.first.is_a?(::Hash)
        Enum.new(self, values.flatten)
      end

      def constructor(fn = nil, **_opts, &block)
        Constructor.new(self, fn || block)
      end
      alias append constructor
      alias >> constructor

      def prepend(fn = nil, &block)
        Constructor.new(self, fn || block)
      end
      alias << prepend

      def |(other)
        Sum.new(self, other)
      end

      def meta(data = Undefined)
        return (@meta || {}) if Undefined.equal?(data)
        dup_with_meta(data)
      end

      def dup_with_meta(data)
        copy = dup
        copy.instance_variable_set(:@meta, (@meta || {}).merge(data))
        copy
      end

      def lax
        self
      end

      def default?
        false
      end

      def optional?
        false
      end

      def constrained?
        false
      end

      def required?
        true
      end

      def to_proc
        method(:call).to_proc
      end

      def name
        primitive.name
      end

      def type
        self
      end

      def strict?
        false
      end
    end

    class Nominal
      include Builder
      attr_reader :primitive

      def initialize(primitive)
        @primitive = primitive
      end

      def call_unsafe(input)
        input
      end

      def primitive?(value)
        value.is_a?(primitive)
      end

      def of(member)
        ArrayMember.new(primitive, member, false)
      end

      def schema(keys = {}, **kw)
        keys = keys.merge(kw)
        Schema.new(primitive, keys)
      end

      def map(key_type, value_type)
        Map.new(self, key_type, value_type)
      end

      def inspect
        "#<Dry::Types[#{primitive}]>"
      end
    end

    class Strict < Nominal
      def self.nominal(primitive)
        new(primitive)
      end

      def call_unsafe(input)
        return input if input.is_a?(primitive)
        raise ConstraintError.new("type?(#{primitive}, #{input.inspect})", input)
      end

      def of(member)
        ArrayMember.new(primitive, member, true)
      end

      def strict?
        true
      end
    end

    class CoercibleType < Nominal
      def initialize(primitive, &fn)
        super(primitive)
        @fn = fn
      end

      def call_unsafe(input)
        @fn.call(input)
      rescue ArgumentError, TypeError, NoMethodError => e
        raise CoercionError, e.message
      end
    end

    class Sum
      include Builder
      attr_reader :left, :right

      def initialize(left, right)
        @left = left
        @right = right
      end

      def call_unsafe(input)
        left.call_unsafe(input)
      rescue CoercionError
        right.call_unsafe(input)
      end

      def optional?
        left.is_a?(Strict) && left.primitive == NilClass
      end

      def primitive
        right.respond_to?(:primitive) ? right.primitive : Object
      end

      def default?
        false
      end

      def name
        "#{left.name} | #{right.name}"
      end
    end

    class Default
      include Builder
      attr_reader :type, :value

      def initialize(type, value)
        @type = type
        @value = value
      end

      def default?
        true
      end

      def evaluate
        @value.is_a?(::Proc) ? (@value.arity.zero? ? @value.call : @value.call(@type)) : @value
      end

      def call_unsafe(input = Undefined)
        if Undefined.equal?(input)
          evaluate
        else
          @type.call_unsafe(input)
        end
      end

      def call(input = Undefined)
        call_unsafe(input)
      end

      def primitive
        @type.primitive
      end

      def optional?
        @type.optional?
      end

      def left
        @type.respond_to?(:left) ? @type.left : nil
      end
    end

    class Constrained
      include Builder
      attr_reader :type, :rules

      def initialize(type, rules)
        @type = type
        @rules = rules
      end

      def constrained?
        true
      end

      def call_unsafe(input)
        v = @type.call_unsafe(input)
        return v if v.nil? && @type.optional?
        @rules.each do |rule, arg|
          ok = case rule
               when :format then v.is_a?(::String) && arg.match?(v)
               when :gteq then v >= arg
               when :gt then v > arg
               when :lteq then v <= arg
               when :lt then v < arg
               when :min_size then v.size >= arg
               when :max_size then v.size <= arg
               when :size then arg.is_a?(::Range) ? arg.include?(v.size) : v.size == arg
               when :included_in then arg.include?(v)
               when :excluded_from then !arg.include?(v)
               when :filled then arg ? !(v.respond_to?(:empty?) && v.empty?) : true
               when :eql, :is then v == arg
               when :min_length then v.size >= arg
               when :max_length then v.size <= arg
               else true
               end
          raise ConstraintError.new("#{rule}?(#{arg.inspect}, #{v.inspect})", input) unless ok
        end
        v
      end

      def primitive
        @type.primitive
      end

      def constrained(**more)
        Constrained.new(@type, @rules.merge(more))
      end

      def default?
        @type.default?
      end

      def optional?
        @type.optional?
      end

      def of(member)
        Constrained.new(@type.of(member), @rules)
      end
    end

    class Enum
      include Builder
      attr_reader :type, :values

      def initialize(type, values)
        @type = type
        @values = values.freeze
      end

      def call_unsafe(input)
        v = @type.call_unsafe(input)
        raise ConstraintError.new("included_in?(#{@values.inspect}, #{input.inspect})", input) unless @values.include?(v)
        v
      end

      def primitive
        @type.primitive
      end

      def include?(v)
        @values.include?(v)
      end

      def default?
        @type.default?
      end

      def optional?
        @type.optional?
      end
    end

    class Constructor
      include Builder
      attr_reader :type, :fn

      def initialize(type, fn)
        @type = type
        @fn = fn
      end

      def call_unsafe(input)
        @type.call_unsafe(@fn.call(input))
      end

      def primitive
        @type.primitive
      end

      def default?
        @type.default?
      end

      def constructor(fn = nil, &block)
        f = fn || block
        inner = @fn
        Constructor.new(@type, ->(x) { f.call(inner.call(x)) })
      end
    end

    class ArrayMember < Nominal
      attr_reader :member

      def initialize(primitive, member, strict)
        super(primitive)
        @member = member
        @strict = strict
      end

      def call_unsafe(input)
        raise ConstraintError.new("type?(Array, #{input.inspect})", input) unless input.is_a?(::Array)
        input.map { |x| @member.call_unsafe(x) }
      end

      def type
        self
      end
    end

    class Map < Nominal
      attr_reader :key_type, :value_type

      def initialize(primitive, key_type, value_type)
        super(primitive)
        @key_type = key_type
        @value_type = value_type
      end

      def call_unsafe(input)
        raise ConstraintError.new("type?(Hash, #{input.inspect})", input) unless input.is_a?(::Hash)
        input.each_with_object({}) { |(k, v), h| h[@key_type.call_unsafe(k)] = @value_type.call_unsafe(v) }
      end
    end

    class Key
      attr_reader :name, :type

      def initialize(name, type, required)
        @name = name
        @type = type
        @required = required
      end

      def required?
        @required
      end

      def omittable?
        !@required
      end

      def default?
        type.default?
      end

      def call_unsafe(v)
        type.call_unsafe(v)
      end
    end

    class Schema < Nominal
      include Enumerable
      attr_reader :keys, :transform_key

      def initialize(primitive, keys = {}, transform_key: nil, base: [])
        super(primitive)
        @transform_key = transform_key
        list = base.dup
        keys.each do |k, t|
          ks = k.to_s
          req = !ks.end_with?('?')
          name = ks.chomp('?').to_sym
          list.reject! { |x| x.name == name }
          list << Key.new(name, t, req)
        end
        @keys = list
      end

      def each(&block)
        @keys.each(&block)
      end

      def map(*_args, &block)
        @keys.map(&block)
      end

      def to_a
        @keys.dup
      end

      def key(name)
        @keys.find { |k| k.name == name }
      end

      def schema(more = {}, **kw)
        Schema.new(primitive, more.merge(kw), transform_key: @transform_key, base: @keys)
      end

      def with_key_transform(fn)
        Schema.new(primitive, {}, transform_key: fn, base: @keys)
      end

      def with_type_transform(_fn)
        self
      end

      def strict(on = true)
        copy = Schema.new(primitive, {}, transform_key: @transform_key, base: @keys)
        copy.instance_variable_set(:@strict, on)
        copy
      end

      def strict?
        @strict ? true : false
      end

      def name_key_map
        @keys.each_with_object({}) { |k, h| h[k.name] = k }
      end

      def apply(input, skip_missing: false, resolve_defaults: true)
        call_unsafe(input, skip_missing: skip_missing, resolve_defaults: resolve_defaults)
      end

      def call_unsafe(input, skip_missing: false, resolve_defaults: true)
        raise ConstraintError.new("type?(Hash, #{input.inspect})", input) unless input.is_a?(::Hash)
        map = name_key_map
        result = {}
        input.each do |k, v|
          tk = @transform_key ? @transform_key.call(k) : k
          key = map[tk]
          raise UnknownKeysError.new([tk]) if key.nil? && @strict
          next unless key
          begin
            result[tk] = key.type.call_unsafe(v)
          rescue ConstraintError, CoercionError => e
            raise SchemaError.new(tk, v, e.message)
          end
        end
        if result.size < @keys.size
          @keys.each do |key|
            next if result.key?(key.name)
            if key.type.default? && resolve_defaults
              result[key.name] = key.type.call_unsafe(Undefined)
            elsif key.required? && !skip_missing
              raise MissingKeyError.new(key.name)
            end
          end
        end
        result
      end
    end

    module BuilderMethods
      def Strict(klass) = Dry::Types::Strict.new(klass)
      def Instance(klass) = Dry::Types::Strict.new(klass)
      def Nominal(klass) = Dry::Types::Nominal.new(klass)
      def Constructor(klass, fn = nil, &block) = Dry::Types::Constructor.new(Dry::Types::Nominal.new(klass), fn || block || klass.method(:new))
      def Array(member) = Dry::Types::ArrayMember.new(::Array, member, true)
      def Hash(map) = Dry::Types::Schema.new(::Hash, map)
      def Value(v) = Dry::Types::Strict.new(v.class).constrained(eql: v)
      def Constant(v) = Value(v)
      def Interface(*methods) = Dry::Types::Nominal.new(::Object).constrained(respond_to: methods)
    end

    def self.build_namespace
      mod = ::Module.new
      mod.extend(BuilderMethods)
      def mod.included(base)
        base.extend(Dry::Types::BuilderMethods)
        super
      end
      strict = ::Module.new
      coercible = ::Module.new
      nominal = ::Module.new
      optional = ::Module.new
      params = ::Module.new
      prims = {
        String: ::String, Integer: ::Integer, Float: ::Float, Symbol: ::Symbol,
        Hash: ::Hash, Array: ::Array, Nil: ::NilClass, Class: ::Class,
        True: ::TrueClass, False: ::FalseClass, Date: ::Object, Time: ::Object,
        DateTime: ::Object, Decimal: ::Float, Range: ::Range
      }
      prims.each do |n, p|
        strict.const_set(n, Strict.new(p))
        nominal.const_set(n, Nominal.new(p))
      end
      bool = Sum.new(Strict.new(::TrueClass), Strict.new(::FalseClass))
      strict.const_set(:Bool, bool)
      nominal.const_set(:Bool, bool)
      any = Nominal.new(::Object)
      coercible.const_set(:String, CoercibleType.new(::String) { |v| Kernel.String(v) })
      coercible.const_set(:Integer, CoercibleType.new(::Integer) { |v| Kernel.Integer(v) })
      coercible.const_set(:Float, CoercibleType.new(::Float) { |v| Kernel.Float(v) })
      coercible.const_set(:Symbol, CoercibleType.new(::Symbol) { |v| v.to_sym })
      coercible.const_set(:Array, CoercibleType.new(::Array) { |v| Kernel.Array(v) })
      coercible.const_set(:Hash, CoercibleType.new(::Hash) { |v| Kernel.Hash(v) })
      coercible.const_set(:Decimal, CoercibleType.new(::Float) { |v| Kernel.Float(v) })
      params.const_set(:Integer, CoercibleType.new(::Integer) { |v| v.is_a?(::String) && v.empty? ? nil : Kernel.Integer(v) })
      params.const_set(:Float, CoercibleType.new(::Float) { |v| Kernel.Float(v) })
      params.const_set(:Bool, CoercibleType.new(::TrueClass) { |v| %w[1 true on yes t y].include?(v.to_s.downcase) ? true : (%w[0 false off no f n].include?(v.to_s.downcase) ? false : raise(ArgumentError, 'not bool')) })
      prims.each_key do |n|
        next if n == :Nil
        optional.const_set(n, strict.const_get(n).optional)
      end
      optional.const_set(:Bool, bool.optional)
      prims.each { |n, _| mod.const_set(n, strict.const_get(n)) }
      mod.const_set(:Bool, bool)
      mod.const_set(:Any, any)
      mod.const_set(:Strict, strict)
      mod.const_set(:Coercible, coercible)
      mod.const_set(:Nominal, nominal)
      mod.const_set(:Optional, optional)
      mod.const_set(:Params, params)
      mod.const_set(:JSON, params)
      mod.const_set(:Undefined, Undefined)
      mod.const_set(:ConstraintError, ConstraintError)
      mod.const_set(:CoercionError, CoercionError)
      mod
    end

    ArrayType = ::Class.new(Nominal)
    ArrayType.const_set(:Member, ArrayMember)
    const_set(:Array, ArrayType)
    HashType = ::Class.new(Nominal)
    HashType.const_set(:Schema, Schema)
    HashType.const_set(:Map, Map)
    const_set(:Hash, HashType)

    REGISTRY = {
      'string' => Strict.new(::String), 'strict.string' => Strict.new(::String),
      'integer' => Strict.new(::Integer), 'strict.integer' => Strict.new(::Integer),
      'bool' => Sum.new(Strict.new(::TrueClass), Strict.new(::FalseClass)),
      'coercible.string' => CoercibleType.new(::String) { |v| Kernel.String(v) },
      'coercible.integer' => CoercibleType.new(::Integer) { |v| Kernel.Integer(v) },
      'coercible.hash' => Nominal.new(::Hash), 'hash' => Strict.new(::Hash),
      'nominal.hash' => Nominal.new(::Hash), 'array' => Strict.new(::Array),
      'any' => Nominal.new(::Object), 'symbol' => Strict.new(::Symbol)
    }.freeze

    def self.[](name)
      REGISTRY.fetch(name.to_s) { raise ArgumentError, "unknown type #{name}" }
    end

    def self.Strict(klass)
      Strict.new(klass)
    end

    def self.Instance(klass)
      Strict.new(klass)
    end

    def self.Nominal(klass)
      Nominal.new(klass)
    end

    def self.Constructor(klass, fn = nil, &block)
      Constructor.new(Nominal.new(klass), fn || block || klass.method(:new))
    end

    def self.Array(member)
      ArrayMember.new(::Array, member, true)
    end

    def self.Hash(map)
      Schema.new(::Hash, map)
    end

    def self.Value(v)
      Strict.new(v.class).constrained(eql: v)
    end
    def self.Constant(v)
      Value(v)
    end
  end

  def self.Types(*_namespaces, default: :strict, **_aliases)
    Dry::Types.build_namespace
  end

  class Struct
    extend Dry::Types::Builder

    class Error < Dry::Types::CoercionError; end
    class RepeatedAttributeError < ArgumentError; end
    class MissingAttributeError < StandardError
      def initialize(attribute:, klass:)
        super("Missing attribute: #{attribute.inspect} on #{klass}")
      end
    end

    module Hashify
      def self.[](value)
        if value.is_a?(Dry::Struct)
          value.to_h
        elsif value.is_a?(::Hash)
          value.each_with_object({}) { |(k, v), h| h[k] = self[v] }
        elsif value.is_a?(::Array)
          value.map { |v| self[v] }
        elsif value.respond_to?(:to_hash) && !value.is_a?(::Hash)
          value.to_hash
        else
          value
        end
      end
    end

    def self.schema(value = Dry::Types::Undefined)
      unless Dry::Types::Undefined.equal?(value)
        @schema = value
        return value
      end
      return @schema if @schema
      @schema = (self == Dry::Struct ? Dry::Types::Schema.new(::Hash, {}) : superclass.schema)
    end

    def self.inherited(klass)
      super
      @__subclasses ||= []
      @__subclasses << klass
      klass.instance_variable_set(:@schema, schema)
    end

    def self.subclasses
      @__subclasses || []
    end

    def self.attribute(name, type = Dry::Types::Undefined, &block)
      attributes(name => build_type(name, type, &block))
    end

    def self.attribute?(*args, &block)
      if args.size == 1 && block.nil?
        return has_attribute?(args[0])
      end
      name, type = args
      attribute(:"#{name}?", build_type(name, type, &block))
    end

    def self.build_type(name, type = Dry::Types::Undefined, &block)
      return Dry::Types[type] if type.is_a?(::String)
      if block
        parent = Dry::Types::Undefined.equal?(type) ? Dry::Struct : type
        nested = ::Class.new(parent.is_a?(::Class) && parent <= Dry::Struct ? parent : Dry::Struct, &block)
        return type.is_a?(Dry::Types::ArrayMember) ? Dry::Types::ArrayMember.new(::Array, nested, true) : nested
      end
      type
    end

    def self.attributes(new_schema)
      keys = new_schema.keys.map { |k| k.to_s.chomp('?').to_sym }
      schema(schema.schema(new_schema))
      keys.each do |key|
        next if method_defined?(key)
        define_method(key) { @attributes[key] }
      end
      @attribute_names = nil
      subclasses.each do |d|
        d.attributes(new_schema.reject { |k, _| d.has_attribute?(k.to_s.chomp('?').to_sym) && false })
      end
      self
    end

    def self.transform_keys(proc = nil, &block)
      schema(schema.with_key_transform(proc || block))
    end

    def self.transform_types(proc = nil, &block)
      self
    end

    def self.attribute_names
      @attribute_names ||= schema.map(&:name)
    end

    def self.has_attribute?(key)
      schema.key(key) ? true : false
    end

    def self.new(attributes = {}, safe = false, &block)
      if attributes.is_a?(Dry::Struct)
        return attributes if equal?(attributes.class)
        return new(attributes.to_h, safe, &block)
      end
      begin
        load(schema.call_unsafe(attributes))
      rescue Dry::Types::CoercionError => e
        raise Error, "[#{self}.new] #{e.message}"
      end
    end

    def self.call_unsafe(input)
      input.is_a?(self) ? input : new(input)
    end

    def self.call(input = Dry::Types::Undefined)
      call_unsafe(input)
    end

    def self.[](input)
      call_unsafe(input)
    end

    def self.load(attributes)
      struct = allocate
      struct.__send__(:initialize, attributes)
      struct
    end

    def self.primitive
      self
    end

    def self.default?
      false
    end

    def self.optional?
      false
    end

    def self.constrained?
      false
    end

    def self.abstract
      nil
    end

    def initialize(attributes)
      @attributes = attributes
    end

    def attributes
      @attributes
    end

    def __attributes__
      @attributes
    end

    def [](name)
      @attributes.fetch(name) do
        if self.class.attribute_names.include?(name)
          nil
        else
          raise MissingAttributeError.new(attribute: name, klass: self.class)
        end
      end
    end

    def to_h
      self.class.schema.each_with_object({}) do |key, result|
        result[key.name] = Hashify[self[key.name]] if attributes.key?(key.name)
      end
    end
    alias to_hash to_h

    def new(changeset)
      new_attributes = self.class.schema.apply(changeset, skip_missing: true, resolve_defaults: false)
      self.class.load(@attributes.merge(new_attributes))
    end

    def ==(other)
      other.class == self.class && other.__attributes__ == @attributes
    end
    alias eql? ==

    def hash
      [self.class, @attributes].hash
    end

    def inspect
      attrs = self.class.attribute_names.map { |key| " #{key}=#{@attributes[key].inspect}" }.join
      "#<#{self.class.name}#{attrs}>"
    end

    def deconstruct_keys(_keys)
      @attributes
    end
  end

  class Struct
    class Value < Struct; end
  end
end

module Kernel
  def Dry
    Dry
  end
end

TOPLEVEL_BINDING = nil
