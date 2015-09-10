package org.vti.jdbc.template;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import com.kdx.core.enums.Status;

public class JdbcTemplateTool {

	private static Log logger = LogFactory.getLog(JdbcTemplateTool.class);
	
	public JdbcTemplate jdbcTemplate;

	public JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}

	public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	@SuppressWarnings("unchecked")
	public int save(Object object) {
			
		SimpleJdbcInsert simpleJdbcInsert=new SimpleJdbcInsert(jdbcTemplate);
		simpleJdbcInsert.withTableName(getTableName(object));
		Map<String, Object> map = null;
		
		try {
			map= PropertyUtils.describe(object);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			logger.error(e);
		} catch (InvocationTargetException e) {
			e.printStackTrace();
			logger.error(e);
		}catch (NoSuchMethodException e) {
			logger.error(e);
		}
		
		if (map!=null) {
		
			Field[] fields = object.getClass().getDeclaredFields();
			
			for (int i = 0; i < fields.length; i++) {
				
				if (fields[i].getAnnotation(Transient.class)!=null) {
					map.remove(fields[i].getName());
					continue;
				}
				
				Column column=fields[i].getAnnotation(Column.class);
				
				Object value=map.get(fields[i].getName());
				
				if (column!=null) {
					
					map.remove(fields[i].getName());
					map.put(column.name(), value);
					
				}else {
					continue;
				}
			}
			
			simpleJdbcInsert.compile();
			return simpleJdbcInsert.execute(map);
			
		}else {
			return 0;
		}
	};

	public int update(String sql,Object[] params) {
		return jdbcTemplate.update(sql,params);
	};

	public int delete(Object object) {
		String sql =getDelSQL(object);
		Object vobj=getTablePKValue(object);
		if (vobj!=null) {
			return jdbcTemplate.update(sql,vobj);
		}else {
			throw new RuntimeException("The primary key dont have a default value in the " +object.getClass());
		}
	};
	
	public int delete(String sql,Object[] params) {
		if (params == null || params.length == 0) {
			return delete(sql);
		}else {
			return jdbcTemplate.update(sql,params);
		}
	};
	
	public <T> T get(Class<T> clazz, Object id) {
		try {
			String sql =getSelectSQL(clazz.newInstance());
			List<T>list=jdbcTemplate.query(sql,new Object[]{id},new BeanPropertyRowMapper<T>(clazz));
			if (list.size()>0) {
				return list.get(0);
			}else {
				return null;
			}
		} catch (Exception e) {
			logger.error(e);
			e.printStackTrace();
			return null;
		}
	};
	
	public <T> T get(String sql, Class<T> clazz) {
		List<T> list = jdbcTemplate.query(sql, new BeanPropertyRowMapper<T>(clazz));
		if (list.size()>0) {
			return list.get(0);
		}else {
			return null;
		}
	};

	public <T> T get(String sql, Object[] params, Class<T> clazz) {
		if (params == null || params.length == 0) {
			return get(sql, clazz);
		} else {
			return jdbcTemplate.query(sql, params,new BeanPropertyRowMapper<T>(clazz)).get(0);
		}
	};

	public <T> List<T> list(String sql, Class<T> clazz) {
		List<T> list = jdbcTemplate.query(sql, new BeanPropertyRowMapper<T>(clazz));
		return list;
	};

	public <T> List<T> list(String sql, Object[] params, Class<T> clazz) {
		List<T> list = null;
		if (params == null || params.length == 0) {
			list = list(sql,clazz);
		} else {
			list = jdbcTemplate.query(sql, params,new BeanPropertyRowMapper<T>(clazz));
		}
		return list;
	};

	public long count(String sql) {
		long count = jdbcTemplate.queryForObject(sql, Long.class);
		return count;
	};

	public long count(String sql, Object[] params) {
		long count = 0;
		if (params == null || params.length == 0) {
			count = count(sql);
		} else {
			count = jdbcTemplate.queryForObject(sql, params, Long.class);
		}
		return count;
	};
	
	private String getSelectSQL(Object object) {
		String sql = "SELECT * FROM " + getTableName(object)+" WHERE " +getTableId(object)+"= ? ";
		return sql;
	};
	
	private String getDelSQL(Object object) {
		String sql = "DELETE FROM " + getTableName(object)+" WHERE "+getTableId(object)+" = ? ";
		return sql;
	};
	
	private String getTableName(Object object){
		Table table = object.getClass().getAnnotation(Table.class);
		if(table != null){
			if(table.catalog() != null){
				return table.catalog() + "." + table.name();
			}else {
				return table.name();
			}
		}else {
			return object.getClass().getSimpleName();
		}
	};
	
	private String getTableId(Object object){
		Field[] fields = object.getClass().getDeclaredFields();
		
		String pk=null;
		for (int i = 0; i < fields.length; i++) {
			if (fields[i].getAnnotation(Transient.class)!=null) {
				continue;
			}
			if (fields[i].getAnnotation(Id.class)!=null) {
				Column column=fields[i].getAnnotation(Column.class);
				if (column!=null) {
					pk=column.name();
				}else {
					pk=fields[i].getName();
				}		
				break;
			}
		}
		
		if (pk!=null) {
			return pk;
		}else {
			throw new RuntimeException("There are Not Found @Id in the " +object.getClass());
		}
	};
	
	private Object getTablePKValue(Object object){
		Field[] fields = object.getClass().getDeclaredFields();
		
		Object vobj=null;
		for (int i = 0; i < fields.length; i++) {
			if (fields[i].getAnnotation(Transient.class)!=null) {
				continue;
			}
			if (fields[i].getAnnotation(Id.class)!=null) {
				
				String fname= fields[i].getName();
				fname=fname.toUpperCase().substring(0,1)+fname.substring(1);
				try {
					Method method=object.getClass().getMethod("get"+fname);
					vobj= method.invoke(object);
					break;
				} catch (Exception e) {
					logger.error(e);
					e.printStackTrace();
					break;
				}
			}else {
				break;
			}
		}
		
		if (vobj!=null) {
			return vobj;
		}else {
			throw new RuntimeException("There are Not Found @Id in the " +object.getClass());
		}
	};
	
}

