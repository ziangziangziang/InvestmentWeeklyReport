---
title: Daily Report
---

A list of daily reports.

<ul>
{% assign days = site.pages | where_exp: "p", "p.path contains 'report/'" | sort: "path" | reverse %}
{% for p in days %}
  {% unless p.name == "index.md" %}
  <li>
    <a href="{{ p.url }}">{{ p.path | split: "/" | last | split: "." | first }}</a>
  </li>
  {% endunless %}
{% endfor %}
</ul>
