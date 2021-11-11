package presentation;


import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

import presentation.AreaService.AreaService;

@ApplicationPath("/rest")
public class PromenadeApplication extends Application {
	
//	@Override
//	public Set<Class<?>> getClasses() {
//		Set<Class<?>> set = new HashSet<Class<?>>();
//		set.add(presentation.AreaService.AreaService.class);
//		return set;
//	}
//
//	@Override
//	public Set<Object> getSingletons() {
//		Set<Object> s = new HashSet<Object>();
//		s.add(new AreaService());
//		return s;
//	}
}