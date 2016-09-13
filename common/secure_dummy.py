# Module for pipeline, for secret functions that shouldn't be in git

def get_norstore_password(process, project_name):
    return process.id

