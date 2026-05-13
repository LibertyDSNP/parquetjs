# Design Documents

* [Design Doc Template](https://github.com/LibertyDSNP/meta/blob/main/templates/DESIGN_DOC_TEMPLATE.md)

## Purposes of this document
* To describe what our design documents are for, their structure, and what goes in them.
* To describe how DSNP Design Documents are produced and where they're published.

## What is a Design Document?
It's a document clearly describing a problem or feature, a solution for it, and why we think the proposal is the right solution.
We'll use _"problem"_ here to broadly mean "anything that means we need to write some code."
It has enough information to begin writing stories for implementing the solution in the document.

## When do we expect a Design Document?
Generally we expect to see a Design Document when the solution is neither simple nor obvious to a developer with domain knowledge of the problem.
If the solution will result in more than one reasonably-sized Issue, that is a sign a Design Document might be needed.

**Examples that don't need a Design Document:** Major revisions to a README file, bug fixes (usually), implementing a login screen.

**Examples that need a Design Document:** A serious security flaw that requires rewriting an entire library, a new API.  

Always use your best judgment.

## Audience
The audience for DSNP Design Documents is mainly DSNP developers from multiple organizations.
We also expect other developers and technically savvy readers will read these documents. 
Linking to technical explainers is encouraged, such as Wikipedia, the DSNP Spec, or other DSNP Design Documents.
This will help readers to learn about concepts in the proposal if need be.

## Sections
There are six required sections and one optional section.
These sections should be in the order that they are described here.

* **Context and Scope**: A short description of the landscape in which the new system is being built, what is actually being built. It may also say what is _not_ being built, and any assumptions. 
    Example:  The proposed feature is a testing library. 
    The context is: the library is for our chosen blockchain. 
    The scope is: this is for a specific repository, so it's not meant to be reused. 
    That means it won't be a separate package, and the code will be tailored for this repo.
    One might also say that the scope is also limited to developer testing, so it's not meant to be used in CI or a test environment such as a test blockchain network.
* **Problem Statement**: The "why." A short summary of the issue(s) that this design solves.
    This doesn't have to be a technical problem, and it doesn't have to be a literal "problem." 
    It could also be a necessary feature.
    "Developer unhappiness", "user experience needs improvement", are also problems.
* **Goals and Non-goals**:  what this solution is trying to do, and is also _not_ trying to do, in concrete terms.  Measurable goals are best.
* **Glossary** (optional): if you think inline links to concepts are too distracting, include a glossary section.
    This can be links, text or both.
* **Proposal**: A high level overview, followed by a detailed description. 
    This is where specific technology, such as language, frameworks, encryption algorithms, type of authentication, and APIs can be described.  
    It can include diagrams such as [a system context diagram](https://en.wikipedia.org/wiki/System_context_diagram), or a [sequence diagram](https://www.geeksforgeeks.org/unified-modeling-language-uml-sequence-diagrams/).
* **Benefits and Risks**: the reasons why this solution was chosen, and the risks this solution poses.
    For example, the solution may be very simple, but there could performance bottlenecks above a certain threshold.
    Another: the solution is well known and widely used, but it's not a perfect fit and requires complicated changes in one area.   
* **Alternatives and Rationale**: discuss alternatives that were considered, and why they were rejected. 
    Note when there are absolute requirements that the solution does not and can't meet.
    One example might be, it's a proprietary solution but we need something open source.
* **Sources**: sources of information that led to this design.

## Process
The process for adding a new DSNP Design Document is much like any other Pull Request in a code base.
1. Create a branch in the appropriate repo for the feature.
2. Commit your design doc in your branch, based on the guidelines here.
3. Post the design document as a Draft PR for discussion.
4. When the PR is accepted, it can be merged.

### Where this fits in the rest of the process
1. Project feature is chosen 
2. Feature requirements are set
3. _Design Doc is created in the appropriate repository using process above_
4. New Issues supporting the Design Doc are filed in the GitHub repo
5. Coding or Writing for an Issue
6. Pull Request is posted that references the Issue

## Writing style
Remember that the first goal of the Design Document is to explain a solution to others so they can understand it well enough to start writing stories and then implement the solution.
The next goal is explain the solution to the public.
Also keep in mind that these documents will be written in English, and some readers may not be fluent in English. 

### General
Be clear, be concise.
Prefer shorter words and simpler sentences.
Break up writing into short paragraphs with one concept each.
Use white space, headings and different text styles to separate and highlight important topics and ideas.

This document is meant to be an example of the preferred writing style.

### Markdown
* Design Docs must be in Markdown format. GitHub Markdown is okay.
* Please put each sentence on its own line in the Markdown.
    This makes diffs for PRs easier to read.
* Inline links are okay.

## Sources
1. [Design docs at Filecoin Project](https://github.com/filecoin-project/designdocs)
2. [Design docs at Google](https://www.industrialempathy.com/posts/design-docs-at-google/)