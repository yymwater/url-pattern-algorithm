package com.flytek.urlpattern.tree;
/**
 * author:Yang Yiming
 * 里面包含相似度高于某个阈值的树节点对，用于url重写
 * **/
public class TreeNodePair {
	private TreeNode sourceNode;
	private TreeNode targetNode;
	private double overlapRate;
	public TreeNodePair(TreeNode sourceNode,TreeNode targetNode,double overlapRate){
		this.sourceNode=sourceNode;
		this.targetNode=targetNode;
		this.overlapRate=overlapRate;
	}
	public TreeNode getSourceNode() {
		return sourceNode;
	}
	public void setSourceNode(TreeNode sourceNode) {
		this.sourceNode = sourceNode;
	}
	public TreeNode getTargetNode() {
		return targetNode;
	}
	public void setTargetNode(TreeNode targetNode) {
		this.targetNode = targetNode;
	}
	public double getOverlapRate() {
		return overlapRate;
	}
	public void setOverlapRate(double overlapRate) {
		this.overlapRate = overlapRate;
	}
}
