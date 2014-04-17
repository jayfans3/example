package com.ailk.oci.ocnosql.client.query.schema;

/**
 * User: Rex wong
 * Date: 13-4-11
 * Time: 上午11:15
 * version
 * since 1.4
 */
public class OciColumn {
    private String name;
    private int position;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }
}
