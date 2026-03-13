# tc_core/utils
from .portabilidade import (
	IS_FROZEN,
	get_assets_path,
	get_base_path,
	get_data_root,
	get_output_path,
	is_shared_data_override_active,
	resolve_data_path,
)

__all__ = [
	"IS_FROZEN",
	"get_base_path",
	"get_assets_path",
	"get_data_root",
	"get_output_path",
	"is_shared_data_override_active",
	"resolve_data_path",
]
