package kits.kitsjdbc;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MapperHelper {

	public static <T, R> List<R> mapToList(Collection<T> list, Function<? super T, ? extends R> mapper) {
		return list.stream().map(mapper).collect(Collectors.toList());
	}
	
}
