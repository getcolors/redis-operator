{ pkgs, ... }:
{
  languages.clojure.enable = true;
  packages = with pkgs; [ babashka kubectl git jq ];
}
