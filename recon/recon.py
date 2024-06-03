# from recon.repo_fs import glob_repos, dir_stats, repo_mod
from recon.repo_db import well_counts, hull_outline
# from recon.epsg import epsg_codes

from recon.repo_fs import glob_repos, dir_stats, repo_mod
from recon.epsg import epsg_codes
from common.util import normalize_path
from common.typeish import validate_repo
from typing import Dict, List, Any

# from common.debugger import debugger


def repo_recon(body) -> List[Dict[str, Any]]:
    """
    :param body: A ReconTaskBody
    :return: A list of valid Repo classes having all expected Repo elements.
    Repo classes are turned back to dicts since they are going into supabase.
    """
    fs_path = normalize_path(body.recon_root)

    repos = glob_repos(fs_path)

    for repo_base in repos:
        for func in [
            well_counts,
            hull_outline,
            epsg_codes,
            dir_stats,
            repo_mod,
        ]:
            md = func(repo_base)
            repo_base.update(md)

    validated_repo_dicts = [validate_repo(r).to_dict() for r in repos]
    return validated_repo_dicts
