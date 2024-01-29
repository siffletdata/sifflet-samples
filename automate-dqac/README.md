# Automate DQAC

## Introduction

The goal of this project is to help automating the creation of DQAC monitors.
It will answer the following problems:

- Having redundant code in the monitor files by adding a "default values" system
- Not being able to see the monitor coverage of a dataset by grouping the monitors by dataset
  inside a single file
- Having to take care of monitors' UUID by generating them automatically and giving coherent names to the monitors
- Not being able to generate monitors from templates inside CI/CD pipelines by providing a CLI

## Installation

First create a virtual environment and install the dependencies:

```bash
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
```

Then, configure a CLI authentication to your tenant and a workspace by following the [official documentation](https://docs.siffletdata.com/docs/cli-command-line-interface).
Add to the `workspace.yaml` file the following key:

```yaml
include:
  - "artefacts/rendered//**/*.yaml"
```

## Usage

### Creating a collection

Collections are a new way of storing DQAC monitors. They consist of folders containing a `$default.yaml` file containing the default values for the collection, yaml files (any name is accepted) containing the monitors and a `README.md` file containing the description of the collection.

To create a collection, simply run the following command:

```bash
python -m sifflet.main create mycollection
```

Then, create at the root of your working directory a `collections.yaml` file containing the following:

```yaml
collections:
  - mycollection
```

now, run the following command, and you should see that Sifflet is reading the collection:

```bash
python -m sifflet.main render collections.yaml
```

### adding monitors to a collection

To add a monitor to a collection, simply create a yaml file inside the collection following
the yaml schema:

```yaml
default_values: <default_values_yaml>
datasets:
  - dataset: <UUID>
    monitors: []
```

Under the `monitors` key, you can add as many monitors as you want, following the yaml schema:

```yaml
- identifier: str
  <DQAC classic yaml structure>
```

Some remarks:

- `identifier`: must be unique inside a collection, and will be used to identify the monitor in Sifflet.
  Modifying the `identifier` key will delete the associated monitor in Sifflet and create a new one.
- `<DQAC classic yaml structure>`: Structure of DQAC monitors, see [official documentation](https://docs.siffletdata.com/docs/monitor-schema) for more information.

You can now register multiple monitors before continuing. Your yaml file will look like this:

```yaml
datasets:
  - dataset: fcc34946-9ef5-438f-9473-99ab692cdac7
    monitors:
      - identifier: monitor1
        kind: Monitor
        version: 1
        name: "[DQAC] Freshness for ORDERS"
        incident:
          severity: Moderate
        parameters:
          kind: Freshness

      - identifier: Completeness_for_fcc34946-9ef5-438f-9473-99ab692cdac7
        kind: Monitor
        version: 1
        name: "[DQAC] Completeness for ORDERS"
        incident:
          severity: Moderate
        parameters:
          kind: Completeness
```

### Rendering collections

Before registering the monitors to Sifflet, collections must be rendered to DQAC yaml files.
To do so, run the following command:

```bash
python -m sifflet.main render collections.yaml
```

This will create a `artefacts/rendered` folder containing the rendered collections. Feel free to check the rendered files before registering the monitors.

You will need to update the `include` key of the `workspace.yaml` file to include the rendered files. It should look like this:

```yaml
include:
  - artefacts/rendered//**/*.yaml
```

Once you are satisfied with the rendered files, you can register the monitors to Sifflet by running:

```bash
sifflet code workspace apply --file workspace.yaml
```

### understanding the $default.yaml file

The `$default.yaml` file is used to store default values for the collection. It is a yaml file that must follow DQAC schema. See [official documentation](https://docs.siffletdata.com/docs/monitor-schema) for more information. Any monitor in the collection can override the default values by specifying the key in the monitors files.

You can now add the following default values to the `$default.yaml` file, and remove them from the previously made monitors:

```yaml
kind: Monitor
version: 1
incident:
  severity: Moderate
```

Now render and register the monitors again, and you should see that the monitors have the same values as before:

```bash
python -m sifflet.main render collections.yaml
sifflet code workspace apply --file workspace.yaml
```

NB: default values can also be specified at file-level, by adding a `default_values` key at the root of the file.

### child collections

Child collections are collections that are stored inside a parent collection. They will inherit the default values of the parent collection, and can override them. They will automatically be detected if the parent collection is registered inside the `collections.yaml` file. Child collections can also have child collections and so on. They are useful to group monitors by datasource or teams for example.

Let's continue our example by creating two child collections:

```bash
python -m sifflet.main create mycollection.teamA
python -m sifflet.main create mycollection.teamB

```

You can now add specific default values to the child collections. For instance, let's add the following default values to `mycollection.teamA`:

```yaml
description: This is the team A collection
```

Now, let's add the following default values to `mycollection.teamB`:

```yaml
description: This is the team B collection
```

Move one of the previously created monitors to `mycollection.teamA` and the other one to `mycollection.teamB`.

Now, render and register the collections, and you should see that each collection has its own description:

```bash
python -m sifflet.main render collections.yaml
sifflet code workspace apply --file workspace.yaml --force_delete
```

Notice that we added the --force_delete flag to the `sifflet code workspace apply` command. This because we moved the monitors to another collection. As monitors must be unique at collection-level, Sifflet will understand that the monitors from the previous collection have been deleted, and new ones have been created. Adding the flag will tell sifflet to delete the previous monitors to prevent duplicates. Consequently,
their history will be lost.

If modifications are made to a monitor without changing its identifier or collection, Sifflet will understand that the monitor has been updated, and will update it instead of deleting it.

### Templates & CI/CD monitors generation

Now that you understand how collections work, you can use them to generate monitors from templates inside CI/CD pipelines. This is useful to generate a set of monitors for each datasource for example.

To do so, we will create a monitor template. Create the file `templates/monitor.j2`, and add inside it:

```jinja2
identifier: {{ name }}
name: "[DQAC] {{ name }}"
parameters:
    kind: "{{ kind }}"
    {% if timestamp_field %}
    timeWindow:
        field: "{{ timestamp_field }}"
        duration: "P1D"
    {% endif %}

```

This is a jinja2 template that will be used to generate monitors. You can now register a monitor to the `mycollection.teamA` collection by running:

```bash
python -m sifflet.main add mycollection.teamA --dataset fcc34946-9ef5-438f-9473-99ab692cdac7 --template templates/monitor.j2 --env name=CUSTOMERS kind=Freshness
```

Now open the monitor file inside the `mycollection.teamA` collection where you have created monitors for a dataset, and you should see that the monitor has been added.

It is also possible to add monitors to datasets that are not yet inside the collection. Run the following command and you should see that a new monitor
file has been created inside the `mycollection.teamB` collection:

```bash
python -m sifflet.main add mycollection.teamB --dataset new_uuid --template templates/monitor.j2 --env name="CUSTOMERS" kind=Freshness
```

Now, render and register the collections, and check that the monitors have been created:

```bash
python -m sifflet.main render collections.yaml
sifflet code workspace apply --file workspace.yaml
```

NB: If the identifier of the monitor generated from template is already present in the collection, adding the flag `--update_monitor`
to the `python -m sifflet.main render collections.yaml` command will replace the monitor inside the collection instead of throwing an error.

## Conclusion

Congratulations, you have successfully created your first collection and generated monitors from templates! You are now ready to automate your DQAC monitors creation.

## Tests

to run tests:

```bash
python -m pytest
```
