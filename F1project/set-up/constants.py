# Databricks notebook source
secret = dbutils.secrets.get(scope="formula1Storage", key="f1accesskey")
containers = ['raw','processed']
storage_account_name = 'martvaformula1dl'
