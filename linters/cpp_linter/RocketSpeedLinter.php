<?php
// Copyright 2015-present Facebook.  All rights reserved.

class RocketSpeedLinter extends ArcanistLinter {
  const LINT_ERROR   = 1;
  const LINT_WARNING = 2;
  const LINT_ADVICE  = 3;

  private $rawLintOutput = array();

  public function getLinterName() {
    return "RocketSpeedLinter";
  }

  public function getLintSeverityMap() {
    return array(
      self::LINT_WARNING => ArcanistLintSeverity::SEVERITY_WARNING,
      self::LINT_ADVICE  => ArcanistLintSeverity::SEVERITY_ADVICE,
      self::LINT_ERROR   => ArcanistLintSeverity::SEVERITY_ERROR
    );
  }

  public function getLintNameMap() {
    return array(
      self::LINT_ADVICE  => "RocketSpeed Advice",
      self::LINT_WARNING => "RocketSpeed Warning",
      self::LINT_ERROR   => "RocketSpeed Error"
    );
  }

  public function willLintPaths(array $paths) {
    $futures = array();
    foreach ($paths as $p) {
      $lpath = $this->getEngine()->getFilePathOnDisk($p);
      $lpath_file = file($lpath);
      if (preg_match('/\.(cc|cpp|h)$/', $lpath)) {
        // Check for the use of assert(...), but ignore static_assert.
        $futures[$p] = new ExecFuture(
          'egrep -n "assert ?\(" %s | grep -v static_assert',
          $lpath);
      }
    }
    foreach (Futures($futures)->limit(8) as $p => $f) {
      $this->rawLintOutput[$p] = $f->resolve();
    }
    return;
  }

  public function lintPath($path) {
    list($stdout, $stderr) = $this->rawLintOutput[$path];

    // Find each line of the grep output.
    // e.g.
    // 287:  assert(false);
    // 298:  assert(ret == 0);
    foreach (explode("\n", $stderr) as $line) {
      // Skip blank lines.
      if (!$line) {
        continue;
      }
      $this->raiseLintAtLine(
        explode(":", $line)[0],                        // line number
        0,                                             // column number
        self::LINT_WARNING,                            // severity
        "Warning: Use RS_ASSERT(...) instead of assert(...)." .
        " RS_ASSERT can be enabled independent of NDEBUG, which allows" .
        " greater control over the build configuration.");  // message
    }
    return;
  }
}
