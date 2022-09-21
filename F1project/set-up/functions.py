# Databricks notebook source
def mount_adls(storage_account, containers, secret):
    for container in containers:
        dbutils.fs.mount(
        source = f"wasbs://{container}@{storage_account_name}.blob.core.windows.net",
        mount_point = f"/mnt/{storage_account_name}/{container}",
        extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": secret}
)

# COMMAND ----------

def unmount_adls(storage_acount_name, containers):
    for container in containers:
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container}")
