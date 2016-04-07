package kits.kitsjdbc;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class NamedParameteredStatementParser {

	public static Result parseNamedParameteredStatement(String namedParameteredStatmentString) {
		List<String> parameterNames = new LinkedList<>();
		
		Pattern pattern = Pattern.compile(":[a-zA-Z_]+\\w*");
		Matcher matcher = pattern.matcher(namedParameteredStatmentString);
		
		while (matcher.find()) {
			String namedParameter = matcher.group();
			String name = namedParameter.substring(1);
			parameterNames.add(name);
		}
		
		String parameteredStatementString = namedParameteredStatmentString.replaceAll(":[a-zA-Z_]+\\w*", "?");
		return new Result(parameteredStatementString, parameterNames);
	}

	static class Result {
		public final String parameteredStatementString;
		public final List<String> parameterNames;
		public Result(String parameteredStatementString, List<String> parameterNames) {
			this.parameteredStatementString = parameteredStatementString;
			this.parameterNames = parameterNames;
		}
		
	}
	
}


