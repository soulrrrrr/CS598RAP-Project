"""Configuration loader for benchmark project."""

import yaml
from pathlib import Path
from typing import Dict, Any


class ConfigLoader:
    """Load and manage benchmark configuration."""

    def __init__(self, config_path: str = "config/benchmark_config.yaml"):
        """
        Initialize configuration loader.

        Args:
            config_path: Path to configuration YAML file
        """
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self._resolve_paths()

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")

        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)

        return config

    def _resolve_paths(self):
        """Convert relative paths to absolute paths."""
        # Get project root
        project_root = Path(self.config['paths']['project_root']).resolve()

        # Resolve all paths
        for key, value in self.config['paths'].items():
            if key != 'project_root':
                self.config['paths'][key] = str(project_root / value)

        self.config['paths']['project_root'] = str(project_root)

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation.

        Args:
            key: Configuration key (e.g., 'benchmark.scale_factor')
            default: Default value if key not found

        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self.config

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value

    def get_all(self) -> Dict[str, Any]:
        """Get entire configuration dictionary."""
        return self.config

    def __getitem__(self, key: str) -> Any:
        """Allow dictionary-style access."""
        return self.config[key]
