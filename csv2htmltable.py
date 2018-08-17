#! /opt/local/bin/python3.6
import os
import pandas as pd


OBSERVATORIES = {
    'south': 'south',
    'north': 'north',
}
NTRIGS = 20


def format_desc(row, gaialink):
    """Format the description with a link to the Gaia website"""
    if row['trigger'].lower().startswith('gaia'):
        return '<a href="{gaialink}{trigger}">{desc}</a>'.format(
            gaialink=gaialink, trigger=row['trigger'], desc=row['description'])
    return ""


def parse(df):
    """Sort the Pandas table, format the link, and select the top NTRIGS"""
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date', ascending=False)
    df = df[:NTRIGS]
    df['trigger'] = df['trigger'].apply(
        lambda x: '<a href="{trigger}.html">{trigger}</a>'.format(trigger=x))
    return df


def format_template(df, dirname):
    """Read a HTML template, insert the CSV HTML table, write index.html"""
    filename = os.path.join(dirname, "template.html")
    with open(filename) as fh:
        html = fh.read()

    pd.set_option('display.max_colwidth', -1)
    table = df.to_html(classes=['table', 'table-striped', 'table-hover'],
                       index=False, escape=False)
    html = html.replace('{{ transients_table }}', table)

    filename = os.path.join(dirname, "index.html")
    with open(filename, 'w') as fh:
        fh.write(html)


def main():
    for key, value in OBSERVATORIES.items():
        dirname = "/home/obrads/www/goto_{}_transients".format(value)
        filename = os.path.join(dirname, "goto_{}.csv".format(key))
        df = pd.read_csv(filename)
        df = parse(df)
        format_template(df, dirname)


if __name__ == '__main__':
    main()
