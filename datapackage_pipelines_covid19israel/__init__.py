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
        check_covid19_israel_id_paths = {}
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
            check_covid19_israel_id_paths[pipeline_id] = source_pipeline["output-dir"]
            pipeline['pipeline'] = [
                {
                    "flow": "avid_covider_pipelines.run_covid19_israel",
                    "parameters": source_pipeline
                }
            ]
            yield os.path.join(source_dir, pipeline_id), pipeline
        failures_report_pipeline = {
            "dependencies": [{"pipeline": "./%s" % id} for id in check_covid19_israel_id_paths.keys()],
            "pipeline": [{
                "flow": "datapackage_pipelines_covid19israel.check_last_failures",
                "parameters": {
                    "check_covid19_israel_id_paths": check_covid19_israel_id_paths
                }
            }]
        }
        yield os.path.join(source_dir, "covid19_israel_failures_report"), failures_report_pipeline
