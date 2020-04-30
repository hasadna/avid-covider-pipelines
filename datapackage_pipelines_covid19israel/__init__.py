import os
from datapackage_pipelines.generators import GeneratorBase, slugify, steps, SCHEDULE_MONTHLY


class Generator(GeneratorBase):

    @classmethod
    def get_schema(cls):
        return {
          "$schema": "http://json-schema.org/draft-04/schema#",
          "type": "object"
        }

    @classmethod
    def generate_pipeline(cls, source, source_dir):
        for pipeline_id, source_pipeline in source.items():
            pipeline = {}
            for source_dependency in source_pipeline.pop('dependencies', []):
                if source_dependency == 'corona_data_collector':
                    dependency = {"pipeline": "./corona_data_collector"}
                elif source_dependency == 'github_pull_covid19_israel':
                    dependency = {"datapackage": "data/github_pull_covid19_israel/datapackage.json"}
                else:
                    dependency = {"pipeline": "./%s" % source_dependency}
                pipeline.setdefault('dependencies', []).append(dependency)
            if not source_pipeline.get("output-dir"):
                source_pipeline["output-dir"] = "data/%s" % pipeline_id
            source_pipeline['raise-exceptions'] = True
            external_sharing_packages = source_pipeline.pop("external_sharing_packages", None)
            pipeline['pipeline'] = [
                {
                    "flow": "avid_covider_pipelines.run_covid19_israel",
                    "parameters": source_pipeline
                }
            ]
            if external_sharing_packages:
                pipeline["pipeline"].append({
                    "flow": "datapackage_pipelines_covid19israel.publish_external_sharing_packages",
                    "parameters": {
                        "packages": external_sharing_packages
                    }
                })
            yield os.path.join(source_dir, pipeline_id), pipeline
