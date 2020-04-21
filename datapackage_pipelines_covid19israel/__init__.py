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
                    dependency = {"pipeline": "./corona_data_collector/export_corona_bot_answers"}
                elif source_dependency == 'github_pull_covid19_israel':
                    dependency = {"datapackage": "data/github_pull_covid19_israel/datapackage.json"}
                else:
                    dependency = {"pipeline": "./%s" % source_dependency}
                pipeline.setdefault('dependencies', []).append(dependency)
            if not source_pipeline.get("output-dir"):
                source_pipeline["output-dir"] = "data/%s" % pipeline_id
            source_pipeline['raise-exceptions'] = True
            pipeline['pipeline'] = [
                {
                    "flow": "avid_covider_pipelines.run_covid19_israel",
                    "parameters": source_pipeline
                }
            ]
            yield os.path.join(source_dir, pipeline_id), pipeline
