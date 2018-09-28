#! /opt/local/bin/python3.6

import os

import pandas as pd


def format_desc(row, gaialink):
    """Format the description with a link to the Gaia website."""
    if row['trigger'].lower().startswith('gaia'):
        return '<a href="{gaialink}{trigger}">{desc}</a>'.format(
            gaialink=gaialink, trigger=row['trigger'], desc=row['description'])
    return ""


def parse(df, ntrigs=20):
    """Sort the Pandas table, format the link, and select the top ntrigs."""
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date', ascending=False)
    df = df[:ntrigs]
    df['trigger'] = df['trigger'].apply(
        lambda x: '<a href="{trigger}.html">{trigger}</a>'.format(trigger=x))
    return df


def format_template(df, file_path):
    """Read a HTML template, insert the CSV HTML table, write index.html."""
    template_file = os.path.join(file_path, "template.html")
    with open(template_file) as f:
        html = f.read()

    pd.set_option('display.max_colwidth', -1)
    table = df.to_html(classes=['table', 'table-striped', 'table-hover'],
                       index=False, escape=False)
    html = html.replace('{{ transients_table }}', table)

    index_file = os.path.join(file_path, "index.html")
    with open(index_file, 'w') as f:
        f.write(html)


def write_table(file_path, csv_file, ntrigs=20):
    """Convert the CSV table into HTML."""
    df = pd.read_csv(os.path.join(file_path, csv_file))
    df = parse(df, ntrigs)
    format_template(df, file_path)
