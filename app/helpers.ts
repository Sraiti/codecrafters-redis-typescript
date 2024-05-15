function redisProtocolParser(str: string) {
  try {
    let index = 0;

    function content() {
      return str.slice(index + 1, str.indexOf("\r\n", index));
    }

    function skip() {
      index = str.indexOf("\r\n", index) + 2;
    }

    function next() {
      let _;

      switch (str[index]) {
        case "+":
          return { message: content() };

        case "-":
          _ = content().split(" ");

          return { name: _[0], message: _.slice(1).join(" ") };

        case ":":
          return Number(content());

        case "$":
          _ = Number(content());

          if (_ === -1) {
            return null;
          }

          skip();

          return str.slice(index, index + _);

        case "*":
          _ = Number(content());

          if (_ === -1) {
            return null;
          }

          _ = new Array(_);

          skip();

          for (let i = 0; i < _.length; i++) {
            _[i] = next();

            skip();
          }

          return _;

        default:
          throw new SyntaxError("Invalid input: " + JSON.stringify(str));
      }
    }

    return next();
  } catch (error) {
    console.log(error);
    return [];
  }
}

function mapToString(map: Map<any, any>): string {
  let result = "";
  for (let [key, value] of map.entries()) {
    if (result !== "") {
      result += "\r\n"; // Add a newline between entries, but not before the first entry
    }
    result += `${key}:${value}`;
  }

  return result;
}

enum Roles {
  SLAVE = "SLAVE",
  MASTER = "MASTER",
}

export { redisProtocolParser, mapToString, Roles };
