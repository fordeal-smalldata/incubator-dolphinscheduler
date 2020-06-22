/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.api.azkaban;

import org.apache.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

/**
 * Hashmap implementation of a hierarchical properties with helpful converter functions and
 * Exception throwing. This class is not threadsafe.
 */
public class Props {

  private final Map<String, String> _current;
  private Props _parent;
  private String source = null;

  public Props() {
    this(null);
  }

  public Props(final Props parent) {
    this._current = new HashMap<>();
    this._parent = parent;
  }

  public Props(final Props parent, final String filepath) throws IOException {
    this(parent, new File(filepath));
  }

  public Props(final Props parent, final File file) throws IOException {
    this(parent);
    setSource(file.getPath());

    final InputStream input = new BufferedInputStream(new FileInputStream(file));
    try {
      loadFrom(input);
    } catch (final IOException e) {
      throw e;
    } finally {
      input.close();
    }
  }

  public Props(final Props parent, final InputStream inputStream) throws IOException {
    this(parent);
    loadFrom(inputStream);
  }

  public Props(final Props parent, final Map<String, String>... props) {
    this(parent);
    for (int i = props.length - 1; i >= 0; i--) {
      this.putAll(props[i]);
    }
  }

  public Props(final Props parent, final Properties... properties) {
    this(parent);
    for (int i = properties.length - 1; i >= 0; i--) {
      this.put(properties[i]);
    }
  }

  public Props(final Props parent, final Props props) {
    this(parent);
    if (props != null) {
      putAll(props);
    }
  }

  /**
   * Create a Props with a null parent from a list of key value pairing. i.e. [key1, value1, key2, value2 ...]
   * @param args args
   * @return props
   */
  public static Props of(final String... args) {
    return of((Props) null, args);
  }

  /**
   * Create a Props from a list of key value pairing. i.e. [key1, value1, key2, value2 ...]
   * @param parent parent
   * @param args args
   * @return props
   */
  public static Props of(final Props parent, final String... args) {
    if (args.length % 2 != 0) {
      throw new IllegalArgumentException(
          "Must have an equal number of keys and values.");
    }

    final Map<String, String> vals = new HashMap<>(args.length / 2);

    for (int i = 0; i < args.length; i += 2) {
      vals.put(args[i], args[i + 1]);
    }
    return new Props(parent, vals);
  }

  public static Props clone(final Props p) {
    return copyNext(p);
  }

  private static Props copyNext(final Props source) {
    Props priorNodeCopy = null;
    if (source.getParent() != null) {
      priorNodeCopy = copyNext(source.getParent());
    }
    final Props dest = new Props(priorNodeCopy);
    for (final String key : source.localKeySet()) {
      dest.put(key, source.get(key));
    }

    return dest;
  }

  private void loadFrom(final InputStream inputStream) throws IOException {
    final Properties properties = new Properties();
    properties.load(new InputStreamReader(inputStream, "utf-8"));
    this.put(properties);
  }

  public Props getEarliestAncestor() {
    if (this._parent == null) {
      return this;
    }

    return this._parent.getEarliestAncestor();
  }

  public void setEarliestAncestor(final Props parent) {
    final Props props = getEarliestAncestor();
    props.setParent(parent);
  }

  public void clearLocal() {
    this._current.clear();
  }

  public boolean containsKey(final Object k) {
    return this._current.containsKey(k)
        || (this._parent != null && this._parent.containsKey(k));
  }

  public boolean containsValue(final Object value) {
    return this._current.containsValue(value)
        || (this._parent != null && this._parent.containsValue(value));
  }

  public String get(final Object key) {
    if (this._current.containsKey(key)) {
      return this._current.get(key);
    } else if (this._parent != null) {
      return this._parent.get(key);
    } else {
      return null;
    }
  }

  public Set<String> localKeySet() {
    return this._current.keySet();
  }

  public Props getParent() {
    return this._parent;
  }

  public void setParent(final Props prop) {
    this._parent = prop;
  }

  public String put(final String key, final String value) {
    return this._current.put(key, value);
  }

  public void put(final Properties properties) {
    for (final String propName : properties.stringPropertyNames()) {
      this._current.put(propName, properties.getProperty(propName));
    }
  }

  public String put(final String key, final Integer value) {
    return this._current.put(key, value.toString());
  }

  public String put(final String key, final Long value) {
    return this._current.put(key, value.toString());
  }

  public String put(final String key, final Double value) {
    return this._current.put(key, value.toString());
  }

  public void putAll(final Map<? extends String, ? extends String> m) {
    if (m == null) {
      return;
    }

    for (final Map.Entry<? extends String, ? extends String> entry : m.entrySet()) {
      this.put(entry.getKey(), entry.getValue());
    }
  }

  public void putAll(final Props p) {
    if (p == null) {
      return;
    }

    for (final String key : p.getKeySet()) {
      this.put(key, p.get(key));
    }
  }

  public void putLocal(final Props p) {
    for (final String key : p.localKeySet()) {
      this.put(key, p.get(key));
    }
  }

  public String removeLocal(final Object s) {
    return this._current.remove(s);
  }

  public int size() {
    return getKeySet().size();
  }

  public int localSize() {
    return this._current.size();
  }

  public Class<?> getClass(final String key) {
    try {
      if (containsKey(key)) {
        return Class.forName(get(key));
      } else {
        throw new UndefinedPropertyException("Missing required property '"
            + key + "'");
      }
    } catch (final ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public Class<?> getClass(final String key, final boolean initialize, final ClassLoader cl) {
    try {
      if (containsKey(key)) {
        return Class.forName(get(key), initialize, cl);
      } else {
        throw new UndefinedPropertyException("Missing required property '"
            + key + "'");
      }
    } catch (final ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public Class<?> getClass(final String key, final Class<?> defaultClass) {
    if (containsKey(key)) {
      return getClass(key);
    } else {
      return defaultClass;
    }
  }

  public String getString(final String key, final String defaultValue) {
    if (containsKey(key)) {
      return get(key);
    } else {
      return defaultValue;
    }
  }

  public String getString(final String key) {
    if (containsKey(key)) {
      return get(key);
    } else {
      throw new UndefinedPropertyException("Missing required property '" + key
          + "'");
    }
  }

  public List<String> getStringList(final String key) {
    return getStringList(key, "\\s*,\\s*");
  }

  public List<String> getStringListFromCluster(final String key) {
    List<String> curlist = getStringList(key, "\\s*;\\s*");
    // remove empty elements in the array
    for (Iterator<String> iter = curlist.listIterator(); iter.hasNext(); ) {
      String a = iter.next();
      if (a.length() == 0) {
        iter.remove();
      }
    }
    return curlist;
  }

  public List<String> getStringList(final String key, final String sep) {
    final String val = get(key);
    if (val == null || val.trim().length() == 0) {
      return Collections.emptyList();
    }

    if (containsKey(key)) {
      return Arrays.asList(val.split(sep));
    } else {
      throw new UndefinedPropertyException("Missing required property '" + key
          + "'");
    }
  }

  public List<String> getStringList(final String key, final List<String> defaultValue) {
    if (containsKey(key)) {
      return getStringList(key);
    } else {
      return defaultValue;
    }
  }

  public List<String> getStringList(final String key, final List<String> defaultValue,
      final String sep) {
    if (containsKey(key)) {
      return getStringList(key, sep);
    } else {
      return defaultValue;
    }
  }

  public boolean getBoolean(final String key, final boolean defaultValue) {
    if (containsKey(key)) {
      return "true".equalsIgnoreCase(get(key).trim());
    } else {
      return defaultValue;
    }
  }

  public boolean getBoolean(final String key) {
    if (containsKey(key)) {
      return "true".equalsIgnoreCase(get(key));
    } else {
      throw new UndefinedPropertyException("Missing required property '" + key
          + "'");
    }
  }

  public long getLong(final String name, final long defaultValue) {
    if (containsKey(name)) {
      return Long.parseLong(get(name));
    } else {
      return defaultValue;
    }
  }

  public long getLong(final String name) {
    if (containsKey(name)) {
      return Long.parseLong(get(name));
    } else {
      throw new UndefinedPropertyException("Missing required property '" + name
          + "'");
    }
  }

  public int getInt(final String name, final int defaultValue) {
    if (containsKey(name)) {
      return Integer.parseInt(get(name).trim());
    } else {
      return defaultValue;
    }
  }

  public int getInt(final String name) {
    if (containsKey(name)) {
      return Integer.parseInt(get(name).trim());
    } else {
      throw new UndefinedPropertyException("Missing required property '" + name
          + "'");
    }
  }

  public double getDouble(final String name, final double defaultValue) {
    if (containsKey(name)) {
      return Double.parseDouble(get(name).trim());
    } else {
      return defaultValue;
    }
  }

  public double getDouble(final String name) {
    if (containsKey(name)) {
      return Double.parseDouble(get(name).trim());
    } else {
      throw new UndefinedPropertyException("Missing required property '" + name
          + "'");
    }
  }

  public URI getUri(final String name) {
    if (containsKey(name)) {
      try {
        return new URI(get(name));
      } catch (final URISyntaxException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    } else {
      throw new UndefinedPropertyException("Missing required property '" + name
          + "'");
    }
  }

  public URI getUri(final String name, final URI defaultValue) {
    if (containsKey(name)) {
      return getUri(name);
    } else {
      return defaultValue;
    }
  }

  public URI getUri(final String name, final String defaultValue) {
    try {
      return getUri(name, new URI(defaultValue));
    } catch (final URISyntaxException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  public void storeLocal(final File file) throws IOException {
    final BufferedOutputStream out =
        new BufferedOutputStream(new FileOutputStream(file));
    try {
      storeLocal(out);
    } finally {
      out.close();
    }
  }

  public Props local() {
    return new Props(null, this._current);
  }

  /**
   * Store only those properties defined at this local level
   *
   * @param out The output stream to write to
   * @throws IOException If the file can't be found or there is an io error
   */
  public void storeLocal(final OutputStream out) throws IOException {
    final Properties p = new Properties();
    for (final String key : this._current.keySet()) {
      p.setProperty(key, get(key));
    }
    p.store(out, null);
  }

  public Properties toProperties() {
    final Properties p = new Properties();
    for (final String key : this._current.keySet()) {
      p.setProperty(key, get(key));
    }

    return p;
  }

  public Properties toAllProperties() {
    Properties allProp = new Properties();
    // import local properties
    allProp.putAll(toProperties());

    // import parent properties
    if(_parent != null)
      allProp.putAll(_parent.toProperties());

    return allProp;
  }

  /**
   * Store all properties, those local and also those in parent props
   *
   * @param file The file to store to
   * @throws IOException If there is an error writing
   */
  public void storeFlattened(final File file) throws IOException {
    final BufferedOutputStream out =
        new BufferedOutputStream(new FileOutputStream(file));
    try {
      storeFlattened(out);
    } finally {
      out.close();
    }
  }

  public void storeFlattened(final OutputStream out) throws IOException {
    final Properties p = new Properties();
    for (Props curr = this; curr != null; curr = curr.getParent()) {
      for (final String key : curr.localKeySet()) {
        if (!p.containsKey(key)) {
          p.setProperty(key, get(key));
        }
      }
    }

    p.store(out, null);
  }

  public Map<String, String> getFlattened() {
    final TreeMap<String, String> returnVal = new TreeMap<>();
    returnVal.putAll(getMapByPrefix(""));
    return returnVal;
  }

  public Map<String, String> getMapByPrefix(final String prefix) {
    final Map<String, String> values = this._parent == null ? new HashMap<>() :
        this._parent.getMapByPrefix(prefix);

    // when there is a conflict, value from the child takes the priority.
    for (final String key : this.localKeySet()) {
      if (key.startsWith(prefix)) {
        values.put(key.substring(prefix.length()), get(key));
      }
    }
    return values;
  }

  public Set<String> getKeySet() {
    final HashSet<String> keySet = new HashSet<>();

    keySet.addAll(localKeySet());

    if (this._parent != null) {
      keySet.addAll(this._parent.getKeySet());
    }

    return keySet;
  }

  public void logProperties(final Logger logger, final String comment) {
    logger.info(comment);

    for (final String key : getKeySet()) {
      logger.info("  key=" + key + " value=" + get(key));
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (o == this) {
      return true;
    } else if (o == null) {
      return false;
    } else if (o.getClass() != Props.class) {
      return false;
    }

    final Props p = (Props) o;
    return this._current.equals(p._current) && equalsObject(this._parent, p._parent);
  }

  public boolean equalsObject(final Object a, final Object b) {
    if (a == null || b == null) {
      return a == b;
    }

    return a.equals(b);
  }

  public boolean equalsProps(final Props p) {
    if (p == null) {
      return false;
    }

    final Set<String> myKeySet = getKeySet();
    for (final String s : myKeySet) {
      if (!get(s).equals(p.get(s))) {
        return false;
      }
    }

    return myKeySet.size() == p.getKeySet().size();
  }

  @Override
  public int hashCode() {
    int code = this._current.hashCode();
    if (this._parent != null) {
      code += this._parent.hashCode();
    }
    return code;
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder("{");
    for (final Map.Entry<String, String> entry : this._current.entrySet()) {
      builder.append(entry.getKey());
      builder.append(": ");
      builder.append(entry.getValue());
      builder.append(", ");
    }
    if (this._parent != null) {
      builder.append(" parent = ");
      builder.append(this._parent.toString());
    }
    builder.append("}");
    return builder.toString();
  }

  public String getSource() {
    return this.source;
  }

  public void setSource(final String source) {
    this.source = source;
  }
}
