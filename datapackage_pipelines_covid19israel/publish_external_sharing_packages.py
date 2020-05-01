import os
import json
from dataflows import Flow, update_resource, printer
import tempfile
import shutil
import logging
from avid_covider_pipelines.utils import get_hash, subprocess_call_log


def flow(parameters, *_):

    def _process_packages():
        for package in parameters.get("packages", []):
            with open(os.path.join("..", "COVID19-ISRAEL", package["package_path"])) as f:
                package_descriptor = json.load(f)
            resources = {
                resource["name"]: resource
                for resource in package_descriptor["resources"]
            }
            for publish_target in package["publish_targets"]:
                assert "github_repo" in publish_target and "deploy_key" in publish_target and "files" in publish_target
                with tempfile.TemporaryDirectory() as tmpdir:
                    source_deploy_key_file = os.environ["DEPLOY_KEY_FILE_" + publish_target["deploy_key"]]
                    deploy_key_file = os.path.join(tmpdir, "deploy_key")
                    shutil.copyfile(source_deploy_key_file, deploy_key_file)
                    os.chmod(deploy_key_file, 0o400)
                    gitenv = {
                        **os.environ,
                        "GIT_SSH_COMMAND": "ssh -i %s -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no" % deploy_key_file
                    }
                    branch = publish_target.get("branch", "master")
                    repodir = os.path.join(tmpdir, "repo")
                    assert subprocess_call_log(["git", "clone", "--depth", "1", "--branch", branch, "git@github.com:hasadna/avid-covider-raw-data.git", repodir], env=gitenv) == 0
                    assert subprocess_call_log(["git", "config", "user.name", "avid-covider-pipelines"], cwd=repodir) == 0
                    assert subprocess_call_log(["git", "config", "user.email", "avid-covider-pipelines@localhost"], cwd=repodir) == 0
                    num_added = 0
                    for resource_name, target_path_template in publish_target["files"].items():
                        target_path = target_path_template.format(**package_descriptor)
                        target_fullpath = os.path.join(repodir, target_path)
                        if os.path.exists(target_fullpath) and get_hash(target_fullpath) == resources[resource_name]["hash"]:
                            logging.info("File is not changed: %s" % resources[resource_name]["path"])
                            continue
                        source_path = os.path.join("..", "COVID19-ISRAEL", resources[resource_name]["path"])
                        logging.info("%s: %s --> %s" % (resource_name, source_path, target_fullpath))
                        shutil.copyfile(source_path, target_fullpath)
                        assert subprocess_call_log(["git", "add", target_path], cwd=repodir) == 0
                        num_added += 1
                    if num_added > 0:
                        logging.info("Committing %s changes" % num_added)
                        assert subprocess_call_log(["git", "commit", "-m", "automated update from hasadna/avid-covider-pipelines"], cwd=repodir) == 0
                        assert subprocess_call_log(["git", "push", "origin", branch], cwd=repodir, env=gitenv) == 0
                    else:
                        logging.info("No changes to commit")
            yield {"name": package_descriptor["name"], "datetime": package_descriptor["datetime"], "hash": package_descriptor["hash"]}

    return Flow(
        _process_packages(),
        update_resource(-1, name="published_packages", path="published_packages.csv", **{"dpp:streaming": True}),
        printer()
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    flow({
        "packages": [
            {
                "package_path": "out/external_sharing/HASADNA/datapackage.json",
                "publish_targets": [
                    {
                        "github_repo": "hasadna/avid-covider-raw-data",
                        "deploy_key": "hasadna_avid_covider_raw_data",
                        "files": {
                            "daily_summary": "input/{POSTERIOR_DATE}.csv",
                            "cities_geojson": "geo/cities.geojson",
                            "neighborhoods_geojson": "geo/neighborhoods.geojson"
                        }
                    }
                ]

            }
        ]
    }).process()
