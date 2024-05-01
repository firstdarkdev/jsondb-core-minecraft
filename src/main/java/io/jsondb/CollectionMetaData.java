/*
 * Copyright (c) 2016 Farooq Khan
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package io.jsondb;

import io.jsondb.annotation.Document;
import io.jsondb.annotation.Id;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @version 1.0 25-Sep-2016
 */
public class CollectionMetaData {
    private final String collectionName;
    private final String schemaVersion;
    private String actualSchemaVersion;
    private final Comparator<String> schemaComparator;
    private final Class<?> clazz;

    private String idAnnotatedFieldName;
    private final Method idAnnotatedFieldGetterMethod;
    private final Method idAnnotatedFieldSetterMethod;

    private final ReentrantReadWriteLock collectionLock;

    private final List<String> secretAnnotatedFieldNames = new ArrayList<>();
    private final Map<String, Method> getterMethodMap = new TreeMap<>();
    private final Map<String, Method> setterMethodMap = new TreeMap<>();

    private boolean hasSecret;
    private boolean readonly;

    public CollectionMetaData(String collectionName, Class<?> clazz, String schemaVersion, Comparator<String> schemaComparator) {
        super();
        this.collectionName = collectionName;
        this.schemaVersion = schemaVersion;
        this.schemaComparator = schemaComparator;
        this.clazz = clazz;

        this.collectionLock = new ReentrantReadWriteLock();

        //Populate the class metadata
        List<Field[]> fields = new ArrayList<>();
        Field[] startFields = clazz.getDeclaredFields();
        int totalFields = startFields.length;
        fields.add(startFields);
        while (clazz.getSuperclass() != Object.class) {
            clazz = clazz.getSuperclass();
            Field[] toAdd = clazz.getDeclaredFields();
            totalFields += toAdd.length;
            fields.add(toAdd);
        }

        int x = 0;
        Field[] fs = new Field[totalFields];
        for (Field[] field : fields) {
            for (Field value : field) {
                fs[x] = value;
                x++;
            }
        }

        Method[] ms = clazz.getDeclaredMethods();
        for (Field f : fs) {
            String fieldName = f.getName();

            Annotation[] annotations = f.getDeclaredAnnotations();
            for (Annotation a : annotations) {
                if (a.annotationType().equals(Id.class)) {
                    //We expect only one @Id annotated field and only one corresponding getter for it
                    //This logic will capture the last @Id annotated field if there are more than one.
                    this.idAnnotatedFieldName = fieldName;
                }
            }

            String getterMethodName = formGetterMethodName(f);
            String setterMethodName = formSetterMethodName(f);
            for (Method m : ms) {
                if (m.getName().equals(getterMethodName)) {
                    this.getterMethodMap.put(fieldName, m);
                }
                if (m.getName().equals(setterMethodName)) {
                    this.setterMethodMap.put(fieldName, m);
                }
            }
        }
        this.idAnnotatedFieldGetterMethod = getterMethodMap.get(idAnnotatedFieldName);
        this.idAnnotatedFieldSetterMethod = setterMethodMap.get(idAnnotatedFieldName);
    }

    protected ReentrantReadWriteLock getCollectionLock() {
        return collectionLock;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public String getActualSchemaVersion() {
        return actualSchemaVersion;
    }

    public void setActualSchemaVersion(String actualSchemaVersion) {
        this.actualSchemaVersion = actualSchemaVersion;
        int compareResult = schemaComparator.compare(schemaVersion, actualSchemaVersion);
        readonly = compareResult != 0;
    }

    @SuppressWarnings("rawtypes")
    public Class getClazz() {
        return clazz;
    }

    public String getIdAnnotatedFieldName() {
        return idAnnotatedFieldName;
    }

    public Method getIdAnnotatedFieldGetterMethod() {
        return idAnnotatedFieldGetterMethod;
    }

    public Method getIdAnnotatedFieldSetterMethod() {
        return idAnnotatedFieldSetterMethod;
    }

    public List<String> getSecretAnnotatedFieldNames() {
        return secretAnnotatedFieldNames;
    }

    public boolean isSecretField(String fieldName) {
        return secretAnnotatedFieldNames.contains(fieldName);
    }

    public Method getGetterMethodForFieldName(String fieldName) {
        return getterMethodMap.get(fieldName);
    }

    public Method getSetterMethodForFieldName(String fieldName) {
        return setterMethodMap.get(fieldName);
    }

    public boolean hasSecret() {
        return hasSecret;
    }

    public boolean isReadOnly() {
        return readonly;
    }

    private String formGetterMethodName(Field field) {
        String fieldName = field.getName();
        if (field.getType().equals(boolean.class)) {
            return "is" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
        } else {
            return "get" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
        }
    }

    private String formSetterMethodName(Field field) {
        String fieldName = field.getName();
        return "set" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
    }

    /**
     * A utility builder method to scan through the specified package and find all classes/POJOs
     * that are annotated with the @Document annotation.
     *
     * @param dbConfig the object that holds all the baseScanPackage and other settings.
     * @return A Map of collection classes/POJOs
     */
    public static Map<String, CollectionMetaData> builder(JsonDBConfig dbConfig, Set<Class<?>> tables) {
        Map<String, CollectionMetaData> collectionMetaData = new LinkedHashMap<>();
        for (Class<?> c : tables) {
            if (!c.isAnnotationPresent(Document.class))
                continue;

            Document d = c.getAnnotation(Document.class);
            String collectionName = d.collection();
            String version = d.schemaVersion();
            CollectionMetaData cmd = new CollectionMetaData(collectionName, c, version, dbConfig.getSchemaComparator());
            collectionMetaData.put(collectionName, cmd);
        }
        return collectionMetaData;
    }
}
