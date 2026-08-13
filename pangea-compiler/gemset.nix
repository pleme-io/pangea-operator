{
  abstract-synthesizer = {
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "1724yzbklcmiiahb7s3y1ir1i0n03b9c3arlib35g85d8hf0h75d";
      type = "gem";
    };
    version = "0.0.15";
  };
  base64 = {
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "0yx9yn47a8lkfcjmigk79fykxvr80r4m1i35q82sxzynpbm7lcr7";
      type = "gem";
    };
    version = "0.3.0";
  };
  bigdecimal = {
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "1g9zi8c4i7g8zz0c3hxrw6mblrjvgn7akys60clb9si7c1k1gljk";
      type = "gem";
    };
    version = "4.1.2";
  };
  concurrent-ruby = {
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      # CVE-2026-54906 (CRITICAL, ReadWriteLock unauthorized lock release +
      # DoS) fixed >= 1.3.7; bumped straight to 1.3.8 (latest as of
      # 2026-07-19) which also closes CVE-2026-54904/CVE-2026-54905 (same
      # advisory family, HIGH/MEDIUM). Hash verified via `nix hash file
      # --type sha256 --base32` against the real
      # https://rubygems.org/downloads/concurrent-ruby-1.3.8.gem — the same
      # method reproduces the PRE-EXISTING 1.3.6 hash byte-for-byte, so the
      # methodology is confirmed correct, not assumed.
      sha256 = "1qfi2ns3zwkgq616fc127xiqhan7g7m7gqpwriwcr34nds1vxwdj";
      type = "gem";
    };
    version = "1.3.8";
  };
  dry-core = {
    dependencies = ["concurrent-ruby" "logger" "zeitwerk"];
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "18cn9s2p7cbgacy0z41h3sf9jvl75vjfmvj774apyffzi3dagi8c";
      type = "gem";
    };
    version = "1.2.0";
  };
  dry-inflector = {
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "1k1dd35sqqqg2abd2g2w78m94pa3mcwvmrsjbkr3hxpn0jxw5c3z";
      type = "gem";
    };
    version = "1.3.1";
  };
  dry-logic = {
    dependencies = ["bigdecimal" "concurrent-ruby" "dry-core" "zeitwerk"];
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "18nf8mbnhgvkw34drj7nmvpx2afmyl2nyzncn3wl3z4h1yyfsvys";
      type = "gem";
    };
    version = "1.6.0";
  };
  dry-struct = {
    dependencies = ["dry-core" "dry-types" "ice_nine" "zeitwerk"];
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "1wc07v0qm8zbblr74w3iy2s74sxpifyfpw9b2x01a9259icnhf03";
      type = "gem";
    };
    version = "1.8.1";
  };
  dry-types = {
    dependencies = ["bigdecimal" "concurrent-ruby" "dry-core" "dry-inflector" "dry-logic" "zeitwerk"];
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "0y7icwaa26ycikz6h97gwd1hji3r280n4yr2kmn5sfgqp76yxsxs";
      type = "gem";
    };
    version = "1.9.1";
  };
  ice_nine = {
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "1nv35qg1rps9fsis28hz2cq2fx1i96795f91q4nmkm934xynll2x";
      type = "gem";
    };
    version = "0.11.2";
  };
  json = {
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      # CVE-2026-54696 (LOW) fixed >= 2.19.9; bumped to 2.21.1 on 2026-07-19.
      # CVE-2026-71847 (LOW) fixed >= 2.21.2; bumped again 2026-08-13.
      #
      # `json` is not in the Gemfile — it arrives as a transitive dependency of
      # pangea-compiler's own gem, which is why the version lives here and in
      # Gemfile.lock rather than in a manifest anyone reads. Native ext
      # (ext/json/ext/{generator,parser}), same build shape as the nio4r/puma
      # C-ext entries — no new build machinery needed.
      sha256 = "0shwgjqbj856mb6m9kgkpy08nhym2gdvc2yaprlimfmky9y3n78z";
      type = "gem";
    };
    version = "2.21.2";
  };
  logger = {
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "00q2zznygpbls8asz5knjvvj2brr3ghmqxgr83xnrdj4rk3xwvhr";
      type = "gem";
    };
    version = "1.7.0";
  };
  mustermann = {
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "163i29mdcr1h0nximk3d51a1fgp7vz3sfasn8p1rjm2d4g3p0qac";
      type = "gem";
    };
    version = "3.1.1";
  };
  nio4r = {
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "18fwy5yqnvgixq3cn0h63lm8jaxsjjxkmj8rhiv8wpzv9271d43c";
      type = "gem";
    };
    version = "2.7.5";
  };
  pangea-akeyless = {
    dependencies = ["dry-struct" "dry-types" "pangea-core" "terraform-synthesizer"];
    groups = ["default"];
    platforms = [];
    source = {
      path = vendor/pangea-akeyless;
      type = "path";
    };
    version = "0.1.0";
  };
  pangea-aws = {
    dependencies = ["dry-struct" "dry-types" "pangea-core" "terraform-synthesizer"];
    groups = ["default"];
    platforms = [];
    source = {
      path = vendor/pangea-aws;
      type = "path";
    };
    version = "0.2.0";
  };
  pangea-azure = {
    dependencies = ["dry-struct" "dry-types" "pangea-core" "terraform-synthesizer"];
    groups = ["default"];
    platforms = [];
    source = {
      path = vendor/pangea-azure;
      type = "path";
    };
    version = "0.1.0";
  };
  pangea-cloudflare = {
    dependencies = ["dry-struct" "dry-types" "pangea-core" "terraform-synthesizer"];
    groups = ["default"];
    platforms = [];
    source = {
      path = vendor/pangea-cloudflare;
      type = "path";
    };
    version = "0.1.0";
  };
  pangea-compiler = {
    dependencies = ["json" "pangea-akeyless" "pangea-aws" "pangea-azure" "pangea-cloudflare" "pangea-core" "pangea-datadog" "pangea-gcp" "pangea-github" "pangea-hcloud" "pangea-kubernetes" "pangea-porkbun" "pangea-splunk" "pangea-spot" "puma" "sinatra" "terraform-synthesizer"];
    groups = ["default"];
    platforms = [];
    source = {
      path = ./.;
      type = "path";
    };
    version = "0.1.0";
  };
  pangea-core = {
    dependencies = ["base64" "dry-struct" "dry-types" "terraform-synthesizer"];
    groups = ["default"];
    platforms = [];
    source = {
      path = vendor/pangea-core;
      type = "path";
    };
    version = "0.3.0";
  };
  pangea-datadog = {
    dependencies = ["dry-struct" "dry-types" "pangea-core" "terraform-synthesizer"];
    groups = ["default"];
    platforms = [];
    source = {
      path = vendor/pangea-datadog;
      type = "path";
    };
    version = "0.1.0";
  };
  pangea-gcp = {
    dependencies = ["dry-struct" "dry-types" "pangea-core" "terraform-synthesizer"];
    groups = ["default"];
    platforms = [];
    source = {
      path = vendor/pangea-gcp;
      type = "path";
    };
    version = "0.1.0";
  };
  pangea-github = {
    dependencies = ["dry-struct" "dry-types" "pangea-core" "terraform-synthesizer"];
    groups = ["default"];
    platforms = [];
    source = {
      path = vendor/pangea-github;
      type = "path";
    };
    version = "0.1.0";
  };
  pangea-hcloud = {
    dependencies = ["dry-struct" "dry-types" "pangea-core" "terraform-synthesizer"];
    groups = ["default"];
    platforms = [];
    source = {
      path = vendor/pangea-hcloud;
      type = "path";
    };
    version = "0.1.0";
  };
  pangea-kubernetes = {
    dependencies = ["dry-struct" "dry-types" "pangea-core" "terraform-synthesizer"];
    groups = ["default"];
    platforms = [];
    source = {
      path = vendor/pangea-kubernetes;
      type = "path";
    };
    version = "0.1.0";
  };
  pangea-porkbun = {
    dependencies = ["dry-struct" "dry-types"];
    groups = ["default"];
    platforms = [];
    source = {
      path = vendor/pangea-porkbun;
      type = "path";
    };
    version = "0.1.0";
  };
  pangea-splunk = {
    dependencies = ["dry-struct" "dry-types" "pangea-core" "terraform-synthesizer"];
    groups = ["default"];
    platforms = [];
    source = {
      path = vendor/pangea-splunk;
      type = "path";
    };
    version = "0.1.0";
  };
  pangea-spot = {
    dependencies = ["dry-struct" "dry-types" "pangea-aws" "pangea-core" "terraform-synthesizer"];
    groups = ["default"];
    platforms = [];
    source = {
      path = vendor/pangea-spot;
      type = "path";
    };
    version = "0.1.0";
  };
  puma = {
    dependencies = ["nio4r"];
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      # CVE-2026-47736/CVE-2026-47737 (HIGH) have no fix in the 6.x line at
      # all (trivy's own fix-range: "~> 7.2.1, >= 8.0.2") — bumped to 8.0.2
      # (satisfies the >= 8.0.2 branch exactly). Gemspec dependency on nio4r
      # is unchanged (still `~> 2.0`, verified against the real published
      # puma-8.0.2.gem metadata), so no ripple into nio4r's own pin. This
      # gem is loaded but never invoked by the embedded operator (puma/
      # sinatra back the now-sunset pangea-compiler HTTP sidecar the
      # embedded-ruby image doesn't boot — see pangea-compiler.gemspec's
      # own description + flake.nix's embedded-image comment), so the
      # major-version jump carries no live behavioral risk to the operator
      # despite touching an unconstrained-by-us upstream API surface.
      sha256 = "1yw6nvkvddriacmva8hm0za0961d6j96dm7zm6748rmyzcfqgvf8";
      type = "gem";
    };
    version = "8.0.2";
  };
  rack = {
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "1hhjy9gcp52dzij05gmidqac8g28ski5xm67prwmdqmjfcgqxmsy";
      type = "gem";
    };
    version = "3.2.6";
  };
  rack-protection = {
    dependencies = ["base64" "logger" "rack"];
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "1b4bamcbpk29i7jvly3i7ayfj69yc1g03gm4s7jgamccvx12hvng";
      type = "gem";
    };
    version = "4.2.1";
  };
  rack-session = {
    dependencies = ["base64" "rack"];
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "1s7zcxlmg88a6dam4aqbgk9xkpy6dkdfqmmcszkkliy3q3w38m2r";
      type = "gem";
    };
    version = "2.1.2";
  };
  sinatra = {
    dependencies = ["logger" "mustermann" "rack" "rack-protection" "rack-session" "tilt"];
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "103h6wjpcqp3i034hi44za2v365yz7qk9s5df8lmasq43nqvkbmp";
      type = "gem";
    };
    version = "4.2.1";
  };
  terraform-synthesizer = {
    dependencies = ["abstract-synthesizer"];
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "01yl1s6xnxn3qh42ybqanxdgcfpppg2cvjk8pka7xcf5hxz9qxda";
      type = "gem";
    };
    version = "0.0.28";
  };
  tilt = {
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "1cvaikq1dcbfl008i16c1pi1gmdax7vfkvmhch64jdkakyk9nnqd";
      type = "gem";
    };
    version = "2.7.0";
  };
  zeitwerk = {
    groups = ["default"];
    platforms = [];
    source = {
      remotes = ["https://rubygems.org"];
      sha256 = "1pbkiwwla5gldgb3saamn91058nl1sq1344l5k36xsh9ih995nnq";
      type = "gem";
    };
    version = "2.7.5";
  };
}
