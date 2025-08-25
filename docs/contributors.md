# Contributors

The **original authors** were:

- Fabian Gotzens (:octicons-mark-github-16: [@fgotzens](https://github.com/fgotzens))
- Heidi Heinrichs (:house: [website](https://www.fz-juelich.de/profile/heinrichs_h))
- Jonas Hörsch (:octicons-mark-github-16: [@coroa](https://github.com/coroa))
- Fabian Hofmann (:octicons-mark-github-16: [@FabianHofmann](https://github.com/FabianHofmann))

The development of `powerplantmatching` was **helped considerably** by in-depth discussions and exchanges of ideas and code with:

- Tom Brown (:octicons-mark-github-16: [@nworbmot](https://github.com/nworbmot))
- Chris Davis (University of Groningen)
- Johannes Friedrich, Roman Hennig, Colin McCormick (World Resources Institute)

**All contributors:**

As of 2025-08-06, the following people have contributed to `powerplantmatching`:

- Carlos Gaete (:octicons-mark-github-16: [@cdgaete](https://github.com/cdgaete))
- Daniel Rüdt (:octicons-mark-github-16: [@daniel-rdt](https://github.com/daniel-rdt))
- Davide Fioriti (:octicons-mark-github-16: [@davide-f](https://github.com/davide-f))
- enioarz (:octicons-mark-github-16: [@enioarz](https://github.com/enioarz))
- Fabian Hofmann (:octicons-mark-github-16: [@FabianHofmann](https://github.com/FabianHofmann))
- Fabian Neumann (:octicons-mark-github-16: [@fneum](https://github.com/fneum))
- Febin Kachirayil (:octicons-mark-github-16: [@FebinKa](https://github.com/FebinKa))
- Fabian Gotzens (:octicons-mark-github-16: [@fgotzens](https://github.com/fgotzens))
- Heinz-Alexander Fuetterer (:octicons-mark-github-16: [@afuetterer](https://github.com/afuetterer))
- Iegor Riepin (:octicons-mark-github-16: [@Irieo](https://github.com/Irieo))
- jensch-dlr (:octicons-mark-github-16: [@jensch-dlr](https://github.com/jensch-dlr))
- Johannes HAMPP (:octicons-mark-github-16: [@euronion](https://github.com/euronion))
- Jonas Hörsch (:octicons-mark-github-16: [@coroa](https://github.com/coroa))
- Leon Schumm (:octicons-mark-github-16: [@energyLS](https://github.com/energyLS))
- Lukas Franken (:octicons-mark-github-16: [@LukasFrankenQ](https://github.com/LukasFrankenQ))
- Lukas Trippe (:octicons-mark-github-16: [@lkstrp](https://github.com/lkstrp))
- Markus Groissböck (:octicons-mark-github-16: [@gincrement](https://github.com/gincrement))
- Martha Frysztacki (:octicons-mark-github-16: [@martacki](https://github.com/martacki))
- Martin Hjelmeland (:octicons-mark-github-16: [@martinhjel](https://github.com/martinhjel))
- Max Parzen (:octicons-mark-github-16: [@pz-max](https://github.com/pz-max))
- rbaard1 (:octicons-mark-github-16: [@rbaard1](https://github.com/rbaard1))
- Thomas Kouroughli (:octicons-mark-github-16: [@Tomkourou](https://github.com/Tomkourou))

<!---
This list is automatically generated from the GitHub contributors page.
Using the following bash script:

#!/bin/bash

REPO="pypsa/powerplantmatching"
GITHUB_API="https://api.github.com"
CONTRIBUTORS_API="$GITHUB_API/repos/$REPO/contributors?per_page=100"

# Optional: GitHub token to increase rate limit
GITHUB_TOKEN=""

# Fetch contributors list
if [ -z "$GITHUB_TOKEN" ]; then
  CONTRIBUTORS=$(curl -s "$CONTRIBUTORS_API")
else
  CONTRIBUTORS=$(curl -s -H "Authorization: token $GITHUB_TOKEN" "$CONTRIBUTORS_API")
fi

# Collect entries in a temp file
TMPFILE=$(mktemp)

# Process each contributor
echo "$CONTRIBUTORS" | jq -r '.[].login' | while read -r LOGIN; do
  if [ -z "$GITHUB_TOKEN" ]; then
    USER=$(curl -s "$GITHUB_API/users/$LOGIN")
  else
    USER=$(curl -s -H "Authorization: token $GITHUB_TOKEN" "$GITHUB_API/users/$LOGIN")
  fi

  NAME=$(echo "$USER" | jq -r '.name')
  if [ "$NAME" == "null" ] || [ -z "$NAME" ]; then
    NAME="$LOGIN"
  fi

  # Format and store line
  echo "$NAME ( :octicons-mark-github-16: [@$LOGIN](https://github.com/$LOGIN) )" >> "$TMPFILE"
done

# Output final sorted list
echo "# Contributors"
echo ""
sort "$TMPFILE"

# Cleanup
rm "$TMPFILE"

--->

Up-to-date statistics and a complete list of contributors to `powerplantmatching` can be found under [GitHub Insights](https://github.com/PyPSA/powerplantmatching/graphs/contributors).
