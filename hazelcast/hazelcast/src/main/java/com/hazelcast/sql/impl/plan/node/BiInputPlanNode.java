/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.plan.node;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * A node having two inputs.
 */
public abstract class BiInputPlanNode extends AbstractPlanNode {
    /** Left input. */
    protected PlanNode left;

    /** Right input. */
    protected PlanNode right;

    protected BiInputPlanNode() {
        // No-op.
    }

    protected BiInputPlanNode(int id, PlanNode left, PlanNode right) {
        super(id);

        this.left = left;
        this.right = right;
    }

    public PlanNode getLeft() {
        return left;
    }

    public PlanNode getRight() {
        return right;
    }

    @Override
    public final void visit(PlanNodeVisitor visitor) {
        right.visit(visitor);
        left.visit(visitor);

        visit0(visitor);
    }

    protected abstract void visit0(PlanNodeVisitor visitor);

    @Override
    protected final void writeData0(ObjectDataOutput out) throws IOException {
        out.writeObject(left);
        out.writeObject(right);

        writeData1(out);
    }

    @Override
    protected final void readData0(ObjectDataInput in) throws IOException {
        left = in.readObject();
        right = in.readObject();

        readData1(in);
    }

    protected void writeData1(ObjectDataOutput out) throws IOException {
        // No-op.
    }

    protected void readData1(ObjectDataInput in) throws IOException {
        // No-op.
    }
}
