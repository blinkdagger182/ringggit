//
//  HomeAIAssistantOverlay.swift
//  dime
//

import MarkdownUI
import Popovers
import SwiftUI
import UIKit

struct HomeAIAssistantOverlay: View {
    private let assistantTitle = "KIRA"

    @ObservedObject var viewModel: HomeAIAssistantViewModel
    @StateObject private var keyboardHeightHelper = KeyboardHeightHelper()

    let namespace: Namespace.ID
    let topInset: CGFloat
    let bottomInset: CGFloat
    let revealProgress: CGFloat
    let isExpanded: Bool
    let homePeekHeight: CGFloat
    @Binding var openAttachmentOnExpand: Bool
    let onCollapse: () -> Void
    let collapseGesture: AnyGesture<DragGesture.Value>?

    @State private var isOrbBreathing = false
    @State private var composerFocused = false
    @State private var composerFocusRequestID = 0
    @State private var chatScrollRequestID = 0
    @State private var showAttachmentSheet = false
    @State private var previewAttachment: AttachmentItem?

    var body: some View {
        GeometryReader { proxy in
            let keyboardOverlap = max(0, keyboardHeightHelper.keyboardHeight - proxy.safeAreaInsets.bottom)
            let visibleHomePadding: CGFloat = isExpanded && keyboardOverlap == 0
                ? max(44, homePeekHeight + 8)
                : 0
            let baseComposerBottomPadding = max(proxy.safeAreaInsets.bottom, bottomInset) + 12 + visibleHomePadding
            let activeComposerBottomPadding: CGFloat = keyboardOverlap > 0 ? 12 : baseComposerBottomPadding

            ZStack(alignment: .bottom) {
                ZStack {
                    LinearGradient(
                        colors: [Color(hex: "F5F2ED"), Color(hex: "FAF8F4"), Color(hex: "F0ECE3"), Color(hex: "EDE9DF")],
                        startPoint: .topLeading, endPoint: .bottomTrailing
                    )
                    RadialGradient(colors: [Color(hex: "C8B94A").opacity(0.18), Color(hex: "C8B94A").opacity(0.06), .clear], center: .bottomLeading, startRadius: 20, endRadius: 380)
                    RadialGradient(colors: [Color(hex: "4A5240").opacity(0.12), Color(hex: "4A5240").opacity(0.04), .clear], center: .bottomTrailing, startRadius: 20, endRadius: 420)
                    RadialGradient(colors: [Color(hex: "5B8C5A").opacity(0.10), .clear], center: .topLeading, startRadius: 20, endRadius: 300)
                }
                .ignoresSafeArea()
                .opacity(revealProgress)

                VStack(spacing: 0) {
                    previewHeader
                        .padding(.horizontal, 24)
                        .padding(.top, max(topInset, proxy.safeAreaInsets.top) + 6)
                        .opacity(revealProgress)

                    messageArea(
                        keyboardOverlap: keyboardOverlap,
                        composerBottomPadding: activeComposerBottomPadding
                    )
                        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
                        .opacity(revealProgress)
                        .offset(y: (1 - revealProgress) * 24)
                }
                .frame(width: proxy.size.width, height: proxy.size.height, alignment: .top)

                composerSection
                    .padding(.bottom, activeComposerBottomPadding)
                    .opacity(revealProgress)
                    .offset(y: (1 - revealProgress) * 20 - keyboardOverlap)
                    .background(
                        LinearGradient(
                            colors: [
                                Color.white.opacity(0),
                                Color.white.opacity(0.82),
                                Color.white.opacity(0.96)
                            ],
                            startPoint: .top,
                            endPoint: .bottom
                        )
                    )
                    .zIndex(4)

                if isExpanded && keyboardOverlap == 0 {
                    bottomHomeGestureRegion(safeBottom: max(proxy.safeAreaInsets.bottom, bottomInset))
                        .zIndex(5)
                }
            }
            .ignoresSafeArea(.keyboard, edges: .bottom)
            .onAppear {
                isOrbBreathing = true
            }
            .onChange(of: isExpanded) { expanded in
                guard expanded else { return }
                guard viewModel.messages.isEmpty else { return }

                DispatchQueue.main.asyncAfter(deadline: .now() + 0.38) {
                    guard isExpanded else { return }
                    focusComposer()
                }
            }
            .onChange(of: openAttachmentOnExpand) { shouldOpen in
                if shouldOpen {
                    openAttachmentOnExpand = false
                    showAttachmentSheet = true
                }
            }
        }
        .fullScreenCover(item: $previewAttachment) { item in
            AttachmentPreviewView(item: item)
        }
        .sheet(isPresented: $showAttachmentSheet) {
            let sheet = HomeAIAttachmentSheet(
                attachments: $viewModel.pendingAttachments,
                onQuickPrompt: { prompt in
                    viewModel.draftMessage = prompt
                    viewModel.sendDraftMessage()
                }
            )
            if #available(iOS 16.0, *) {
                sheet
                    .presentationDetents([.medium])
                    .presentationDragIndicator(.hidden)
            } else {
                sheet
            }
        }
    }

    private var headerTitle: String {
        viewModel.activeWorkspace.bucketID == nil ? "KIRA" : viewModel.activeWorkspace.title
    }

    private var previewHeader: some View {
        HStack(spacing: 7) {
            Image(systemName: "sparkles")
                .font(Font.satoshi(18, weight: .semibold))
                .foregroundStyle(Color(hex: "4A5240").opacity(0.75))

            Text(headerTitle)
                .font(Font.satoshi(19, weight: .bold))
                .foregroundStyle(Color(hex: "1C1C1A"))

            Text("beta")
                .font(Font.satoshi(12, weight: .medium))
                .foregroundStyle(Color(hex: "4A5240"))
                .padding(.horizontal, 9)
                .padding(.vertical, 4)
                .background(Color(hex: "4A5240").opacity(0.08), in: Capsule())
                .overlay(
                    Capsule()
                        .stroke(Color(hex: "4A5240").opacity(0.20), lineWidth: 1)
                )
        }
        .frame(height: 36)
        .frame(maxWidth: .infinity)
    }

    private func messageArea(keyboardOverlap: CGFloat, composerBottomPadding: CGFloat) -> some View {
        ScrollViewReader { proxy in
            ScrollView(showsIndicators: false) {
                LazyVStack(alignment: .leading, spacing: 16) {
                    if viewModel.messages.isEmpty {
                        emptyState
                    }

                    ForEach(viewModel.messages) { message in
                        messageBubble(message)
                            .id(message.id)
                    }

                    Color.clear
                        .frame(
                            maxWidth: .infinity,
                            minHeight: bottomScrollSpacerHeight(
                                keyboardOverlap: keyboardOverlap,
                                composerBottomPadding: composerBottomPadding
                            ),
                            maxHeight: bottomScrollSpacerHeight(
                                keyboardOverlap: keyboardOverlap,
                                composerBottomPadding: composerBottomPadding
                            )
                        )
                        .contentShape(Rectangle())
                        .onTapGesture {
                            dismissKeyboard()
                        }
                        .id("bottom-anchor")
                }
                .background(
                    Color.black.opacity(0.001)
                        .contentShape(Rectangle())
                        .onTapGesture {
                            dismissKeyboard()
                        }
                )
                .padding(.horizontal, 24)
                .padding(.top, 16)
            }
            .modifier(HomeAIInteractiveKeyboardDismissModifier())
            .background(
                nonBubbleDragArea(height: 1)
            )
            .onChange(of: viewModel.messages.count) { _ in
                requestScrollToChatEnd(proxy, animated: false)
            }
            .onChange(of: viewModel.streamingText) { _ in
                guard !viewModel.streamingText.isEmpty else { return }
                requestScrollToChatEnd(proxy, animated: true)
            }
            .onChange(of: viewModel.messageContentRevision) { _ in
                guard !viewModel.isStreaming else { return }
                requestScrollToChatEnd(proxy, animated: false, delay: 0.02)
            }
            .onChange(of: keyboardHeightHelper.keyboardHeight) { _ in
                requestScrollToChatEnd(proxy, animated: false, delay: 0.18)
            }
            .onAppear {
                guard !viewModel.messages.isEmpty else { return }
                requestScrollToChatEnd(proxy, animated: false, delay: 0)
            }
        }
    }

    private func nonBubbleDragArea(height: CGFloat) -> some View {
        Color.black.opacity(0.001)
            .frame(maxWidth: .infinity, minHeight: height, maxHeight: height)
            .contentShape(Rectangle())
            .onTapGesture {
                dismissKeyboard()
            }
    }

    private var emptyState: some View {
        VStack(spacing: 0) {
            heroCopy
                .padding(.top, 20)

            glowOrb
                .padding(.top, 16)

            suggestionsList
                .padding(.top, 20)
                .padding(.horizontal, 4)
        }
    }

    private var heroCopy: some View {
        VStack(spacing: 10) {
            Text("What can I\nhelp you with today?")
                .font(Font.satoshi(26, weight: .bold))
                .multilineTextAlignment(.center)
                .lineSpacing(3)
                .foregroundStyle(
                    LinearGradient(
                        colors: [Color(hex: "4A5240"), Color(hex: "C8B94A")],
                        startPoint: .leading, endPoint: .trailing
                    )
                )

            Text("Ask anything about your money.\nI'm here to help.")
                .font(Font.satoshi(14, weight: .medium))
                .multilineTextAlignment(.center)
                .lineSpacing(3)
                .foregroundStyle(Color(hex: "6B6B66"))
        }
    }

    private var glowOrb: some View {
        ZStack {
            Ellipse()
                .fill(Color(hex: "4A5240").opacity(isOrbBreathing ? 0.12 : 0.06))
                .frame(
                    width: isOrbBreathing ? 130 : 108,
                    height: isOrbBreathing ? 22 : 16
                )
                .blur(radius: isOrbBreathing ? 10 : 6)
                .offset(y: 26)

            Circle()
                .fill(
                    RadialGradient(
                        colors: [
                            Color.white.opacity(0.90),
                            Color(hex: "C8B94A").opacity(isOrbBreathing ? 0.64 : 0.48),
                            Color(hex: "4A5240").opacity(isOrbBreathing ? 0.78 : 0.58),
                            Color.clear
                        ],
                        center: .center,
                        startRadius: 0,
                        endRadius: isOrbBreathing ? 44 : 36
                    )
                )
                .frame(
                    width: isOrbBreathing ? 68 : 58,
                    height: isOrbBreathing ? 68 : 58
                )
                .blur(radius: isOrbBreathing ? 4 : 2.5)
                .scaleEffect(isOrbBreathing ? 1.06 : 0.96)
                .shadow(color: Color(hex: "C8B94A").opacity(isOrbBreathing ? 0.28 : 0.14), radius: isOrbBreathing ? 20 : 10, x: 0, y: 0)
                .shadow(color: Color(hex: "4A5240").opacity(isOrbBreathing ? 0.32 : 0.16), radius: isOrbBreathing ? 24 : 12, x: 0, y: 6)

            Image(systemName: "sparkles")
                .font(Font.satoshi(isOrbBreathing ? 26 : 22, weight: .semibold))
                .foregroundStyle(Color(hex: "4A5240").opacity(isOrbBreathing ? 0.86 : 0.68))
                .scaleEffect(isOrbBreathing ? 1.08 : 0.92)
        }
        .frame(height: 72)
        .animation(.easeInOut(duration: 1.75).repeatForever(autoreverses: true), value: isOrbBreathing)
    }

    private var suggestionsList: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Suggested for you")
                .font(Font.satoshi(13, weight: .medium))
                .foregroundStyle(Color(hex: "6B6B66"))
                .padding(.leading, 4)
                .padding(.bottom, 2)

            suggestionRow(icon: "chart.line.uptrend.xyaxis", title: "Why did I overspend this week?", tint: Color(hex: "4A5240"))
            suggestionRow(icon: "viewfinder", title: "Scan a receipt", tint: Color(hex: "5B8C5A"))
            suggestionRow(icon: "folder", title: "Create a food budget", tint: Color(hex: "C8B94A"))
            suggestionRow(icon: "arrow.triangle.2.circlepath", title: "Show my subscriptions", tint: Color(hex: "4A5240"))
        }
    }

    private func suggestionRow(icon: String, title: String, tint: Color) -> some View {
        Button {
            viewModel.draftMessage = title
            viewModel.sendDraftMessage()
        } label: {
            HStack(spacing: 13) {
                Circle()
                    .fill(tint.opacity(0.16))
                    .frame(width: 36, height: 36)
                    .overlay(
                        Image(systemName: icon)
                            .font(Font.satoshi(15, weight: .semibold))
                            .foregroundStyle(tint.opacity(0.78))
                    )

                Text(title)
                    .font(Font.satoshi(14, weight: .medium))
                    .foregroundStyle(Color(hex: "1C1C1A"))
                    .lineLimit(1)
                    .minimumScaleFactor(0.86)

                Spacer()

                Image(systemName: "chevron.right")
                    .font(Font.satoshi(13, weight: .semibold))
                    .foregroundStyle(Color(hex: "4A5240").opacity(0.45))
            }
            .padding(.horizontal, 14)
            .frame(height: 48)
            .background(.white.opacity(0.82), in: Capsule())
            .shadow(color: Color(hex: "4A5240").opacity(0.06), radius: 10, x: 0, y: 5)
        }
        .buttonStyle(.plain)
    }

    private func messageBubble(_ message: HomeAIMessage) -> some View {
        HStack(alignment: .top) {
            if message.role == .assistant {
                VStack(alignment: .leading, spacing: 10) {
                    HStack(alignment: .top, spacing: 10) {
                        ZStack {
                            Circle()
                                .fill(Color(hex: "4A5240").opacity(0.12))
                                .frame(width: 30, height: 30)
                            Image(systemName: "sparkles")
                                .font(Font.satoshi(13, weight: .semibold))
                                .foregroundStyle(Color(hex: "4A5240"))
                        }
                        .padding(.top, 2)

                        VStack(alignment: .leading, spacing: 8) {
                            Text("KIRA")
                                .font(Font.satoshi(.subheadline, weight: .semibold))
                                .foregroundStyle(Color(hex: "4A5240"))

                            // ThinkingDisclosure commented out
                            // if let thinking = message.thinking {
                            //     ThinkingDisclosure(thinking: thinking)
                            // }

                            if !message.text.isEmpty {
                                HomeAIMarkdownText(text: message.text)
                            } else if isStreamingAssistantPlaceholder(message) {
                                HStack(spacing: 8) {
                                    Text(viewModel.statusText.isEmpty ? "Thinking" : viewModel.statusText)
                                        .font(Font.satoshi(.body))
                                        .foregroundColor(Color(.secondaryLabel))
                                        .lineSpacing(5)
                                    AIThinkingDots()
                                }
                            }
                        }
                    }

                    if let visual = message.visual {
                        AIVisualCardView(visual: visual)
                    }
                }

                Spacer(minLength: 42)
            } else {
                Spacer(minLength: 42)

                let hasText = !message.text.isEmpty && message.text != "Please extract all transactions from the attached image(s)."
                let firstAttachment = message.attachments.first

                VStack(alignment: .trailing, spacing: 0) {
                    if firstAttachment != nil || hasText {
                        VStack(alignment: .leading, spacing: 0) {
                            if let attachment = firstAttachment {
                                Button { previewAttachment = attachment } label: {
                                    Image(uiImage: attachment.thumbnail)
                                        .resizable()
                                        .scaledToFill()
                                        .frame(width: 240, height: 170)
                                        .clipped()
                                        .overlay(alignment: .bottomLeading) {
                                            if attachment.pages.count > 1 {
                                                Text(attachment.label.components(separatedBy: "·").last?.trimmingCharacters(in: .whitespaces) ?? "")
                                                    .font(Font.satoshi(10, weight: .semibold))
                                                    .foregroundColor(.white)
                                                    .padding(.horizontal, 6)
                                                    .padding(.vertical, 3)
                                                    .background(Color.black.opacity(0.55))
                                                    .clipShape(Capsule())
                                                    .padding(6)
                                            }
                                        }
                                }
                                .buttonStyle(.plain)
                            }

                            if hasText {
                                Text(message.text)
                                    .font(Font.satoshi(.body, weight: .medium))
                                    .foregroundColor(.white)
                                    .padding(.horizontal, 14)
                                    .padding(.vertical, 11)
                            }
                        }
                        .background(
                            LinearGradient(
                                colors: [Color(hex: "4A5240"), Color(hex: "C8B94A")],
                                startPoint: .topLeading,
                                endPoint: .bottomTrailing
                            )
                        )
                        .clipShape(RoundedRectangle(cornerRadius: 16, style: .continuous))
                        .frame(maxWidth: 260, alignment: .trailing)
                    }
                }
            }
        }
    }

    private func isStreamingAssistantPlaceholder(_ message: HomeAIMessage) -> Bool {
        message.role == .assistant
            && message.id == viewModel.messages.last?.id
            && message.text.isEmpty
            && viewModel.isStreaming
    }

    private var composerSection: some View {
        VStack(spacing: 10) {
            if !viewModel.isStreaming {
                composerQuickPrompts
            }

            HStack(alignment: .bottom, spacing: 10) {
                if keyboardHeightHelper.keyboardHeight > 0 {
                    Button { showAttachmentSheet = true } label: {
                        ZStack {
                            Circle()
                                .fill(
                                    LinearGradient(
                                        colors: [Color(hex: "4A5240"), Color(hex: "C8B94A")],
                                        startPoint: .topLeading, endPoint: .bottomTrailing
                                    )
                                )
                                .frame(width: 50, height: 50)
                            Image(systemName: "plus")
                                .font(Font.satoshi(20, weight: .medium))
                                .foregroundColor(.white)
                        }
                    }
                    .buttonStyle(.plain)
                    .padding(.bottom, 3)
                }

                composer
            }
        }
        .padding(.horizontal, 20)
    }

    private var composerQuickPrompts: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(spacing: 8) {
                ForEach(viewModel.quickActions) { quickAction in
                    Button {
                        viewModel.triggerQuickAction(quickAction)
                    } label: {
                        HStack(spacing: 6) {
                            Image(systemName: quickAction.systemImage)
                                .font(Font.satoshi(11, weight: .semibold))
                            Text(quickAction.title)
                                .font(Font.satoshi(.footnote, weight: .medium))
                                .lineLimit(1)
                        }
                        .foregroundColor(Color(hex: "1C1C1A"))
                        .padding(.horizontal, 12)
                        .padding(.vertical, 8)
                        .background(Color.white.opacity(0.78), in: Capsule(style: .continuous))
                        .overlay(
                            Capsule(style: .continuous)
                                .stroke(Color(hex: "4A5240").opacity(0.18), lineWidth: 1)
                        )
                    }
                    .buttonStyle(.plain)
                }
            }
            .padding(.horizontal, 2)
        }
        .scrollClipDisabledIfAvailable()
    }

    private var composer: some View {
        VStack(spacing: 0) {
            // Attachment thumbnails inside the pill
            if !viewModel.pendingAttachments.isEmpty {
                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: 8) {
                        ForEach(viewModel.pendingAttachments) { item in
                            composerAttachmentCell(item)
                        }
                    }
                    .padding(.horizontal, 14)
                    .padding(.vertical, 10)
                }
                .frame(height: 90)

                Rectangle()
                    .fill(Color(.separator).opacity(0.5))
                    .frame(height: 1)
                    .padding(.horizontal, 14)
            }

            VStack(spacing: 12) {
                HStack(alignment: .center, spacing: 12) {
                    HomeAIComposerTextField(
                        text: $viewModel.draftMessage,
                        isFocused: composerFocusBinding,
                        focusRequestID: composerFocusRequestID,
                        placeholder: "Ask KIRA",
                        onSubmit: sendComposerMessage,
                        textColor: .label,
                        placeholderColor: .secondaryLabel
                    )
                    .frame(maxWidth: .infinity, minHeight: 24, maxHeight: 24, alignment: .leading)

                    if viewModel.hasContent {
                        Button { sendComposerMessage() } label: {
                            Image(systemName: "arrow.up.circle.fill")
                                .font(Font.satoshi(26, weight: .semibold))
                                .foregroundStyle(Color(hex: "4A5240"))
                        }
                        .buttonStyle(.plain)
                    }
                }
                .frame(height: 24)

                HStack(spacing: 20) {
                    Button { showAttachmentSheet = true } label: {
                        Image(systemName: "paperclip")
                            .font(Font.satoshi(18, weight: .medium))
                            .foregroundColor(Color(hex: "4A5240").opacity(0.70))
                    }
                    .buttonStyle(.plain)

                    Button { showAttachmentSheet = true } label: {
                        Image(systemName: "camera")
                            .font(Font.satoshi(18, weight: .medium))
                            .foregroundColor(Color(hex: "4A5240").opacity(0.70))
                    }
                    .buttonStyle(.plain)

                    Button {
                        viewModel.draftMessage = viewModel.draftMessage + "@"
                        focusComposer()
                    } label: {
                        Image(systemName: "at")
                            .font(Font.satoshi(18, weight: .medium))
                            .foregroundColor(Color(hex: "4A5240").opacity(0.70))
                    }
                    .buttonStyle(.plain)

                    Button {
                        if viewModel.draftMessage.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                            viewModel.draftMessage = "What can KIRA do?"
                        }
                        focusComposer()
                    } label: {
                        Image(systemName: "sparkles")
                            .font(Font.satoshi(18, weight: .medium))
                            .foregroundColor(Color(hex: "4A5240").opacity(0.70))
                    }
                    .buttonStyle(.plain)

                    Spacer(minLength: 0)
                }
                .frame(height: 20)
            }
            .padding(.horizontal, 16)
            .padding(.vertical, 16)
        }
        .contentShape(Rectangle())
        .onTapGesture { focusComposer() }
        .background(
            RoundedRectangle(cornerRadius: 24, style: .continuous)
                .fill(Color.white.opacity(0.82))
                .overlay(
                    RoundedRectangle(cornerRadius: 24, style: .continuous)
                        .stroke(composerFocused ? Color(hex: "4A5240").opacity(0.40) : Color(hex: "4A5240").opacity(0.18), lineWidth: 1)
                )
                .shadow(color: Color(hex: "4A5240").opacity(composerFocused ? 0.10 : 0.05), radius: composerFocused ? 22 : 12, y: 8)
        )
        .simultaneousGesture(
            DragGesture(minimumDistance: 4)
                .onChanged { value in
                    guard value.translation.height > 8 else { return }
                    dismissKeyboard()
                }
        )
    }

    private func composerAttachmentCell(_ item: AttachmentItem) -> some View {
        ZStack(alignment: .topTrailing) {
            Button { previewAttachment = item } label: {
                ZStack(alignment: .bottomLeading) {
                    Image(uiImage: item.thumbnail)
                        .resizable()
                        .scaledToFill()
                        .frame(width: 68, height: 68)
                        .clipShape(RoundedRectangle(cornerRadius: 10, style: .continuous))

                    if item.pages.count > 1 {
                        Text(item.label.components(separatedBy: "·").last?.trimmingCharacters(in: .whitespaces) ?? "")
                            .font(Font.satoshi(9, weight: .semibold))
                            .foregroundColor(.white)
                            .padding(.horizontal, 5)
                            .padding(.vertical, 2)
                            .background(Color.black.opacity(0.6))
                            .clipShape(Capsule())
                            .padding(4)
                    }
                }
            }
            .buttonStyle(.plain)

            Button { viewModel.removeAttachment(id: item.id) } label: {
                ZStack {
                    Circle()
                        .fill(Color(hex: "2C2C2C"))
                        .frame(width: 20, height: 20)
                    Image(systemName: "xmark")
                        .font(Font.satoshi(9, weight: .bold))
                        .foregroundColor(.white)
                }
                .padding(3)
            }
            .buttonStyle(.plain)
        }
    }

    private func bottomHomeGestureRegion(safeBottom: CGFloat) -> some View {
        let interactiveHeight = max(homePeekHeight, safeBottom + 52)

        return Color.black.opacity(0.001)
        .frame(maxWidth: .infinity)
        .frame(height: interactiveHeight, alignment: .top)
        .contentShape(Rectangle())
        .onTapGesture(perform: onCollapse)
        .modifier(HomeAICollapseGestureModifier(dragGesture: collapseGesture))
        .accessibilityLabel("Swipe up to return Home")
    }

    private func requestScrollToChatEnd(_ proxy: ScrollViewProxy, animated: Bool, delay: TimeInterval = 0) {
        chatScrollRequestID += 1
        let requestID = chatScrollRequestID

        let performScroll = {
            guard requestID == chatScrollRequestID else { return }

            let action = {
                proxy.scrollTo("bottom-anchor", anchor: .bottom)
            }

            if animated {
                withAnimation(.linear(duration: 0.08)) {
                    action()
                }
            } else {
                action()
            }
        }

        if delay == 0 {
            DispatchQueue.main.async(execute: performScroll)
        } else {
            DispatchQueue.main.asyncAfter(deadline: .now() + delay, execute: performScroll)
        }
    }

    private func sendComposerMessage() {
        guard !viewModel.draftMessage.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return }
        guard !viewModel.isStreaming else { return }

        viewModel.sendDraftMessage()
    }

    private func dismissKeyboard() {
        composerFocused = false
        UIApplication.shared.endEditing()
    }

    private func bottomScrollSpacerHeight(keyboardOverlap: CGFloat, composerBottomPadding: CGFloat) -> CGFloat {
        72 + composerBottomPadding + keyboardOverlap
    }

    private func focusComposer() {
        composerFocused = true
        composerFocusRequestID += 1
    }

    private var composerFocusBinding: Binding<Bool> {
        Binding(
            get: { composerFocused },
            set: { composerFocused = $0 }
        )
    }
}

private struct AttachmentPreviewView: View {
    let item: AttachmentItem
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        ZStack(alignment: .topTrailing) {
            Color.black.ignoresSafeArea()

            if item.pages.count == 1 {
                Image(uiImage: item.pages[0])
                    .resizable()
                    .scaledToFit()
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
            } else {
                TabView {
                    ForEach(item.pages.indices, id: \.self) { i in
                        Image(uiImage: item.pages[i])
                            .resizable()
                            .scaledToFit()
                            .frame(maxWidth: .infinity, maxHeight: .infinity)
                            .tag(i)
                    }
                }
                .tabViewStyle(.page)
                .indexViewStyle(.page(backgroundDisplayMode: .always))
            }

            Button { dismiss() } label: {
                ZStack {
                    Circle()
                        .fill(Color.white.opacity(0.15))
                        .frame(width: 36, height: 36)
                    Image(systemName: "xmark")
                        .font(Font.satoshi(14, weight: .semibold))
                        .foregroundColor(.white)
                }
            }
            .buttonStyle(.plain)
            .padding(.top, 56)
            .padding(.trailing, 20)
        }
    }
}

private struct HomeAICollapseGestureModifier: ViewModifier {
    let dragGesture: AnyGesture<DragGesture.Value>?

    func body(content: Content) -> some View {
        if let dragGesture {
            content.highPriorityGesture(dragGesture)
        } else {
            content
        }
    }
}

private struct HomeAIInteractiveKeyboardDismissModifier: ViewModifier {
    func body(content: Content) -> some View {
        if #available(iOS 16.0, *) {
            content.scrollDismissesKeyboard(.interactively)
        } else {
            content
        }
    }
}

private extension View {
    @ViewBuilder
    func scrollClipDisabledIfAvailable() -> some View {
        if #available(iOS 17.0, *) {
            self.scrollClipDisabled()
        } else {
            self
        }
    }
}

private struct HomeAIComposerTextField: UIViewRepresentable {
    @Binding var text: String
    @Binding var isFocused: Bool

    let focusRequestID: Int
    let placeholder: String
    let onSubmit: () -> Void
    let textColor: UIColor
    let placeholderColor: UIColor

    func makeUIView(context: Context) -> UITextField {
        let textField = UITextField(frame: .zero)
        textField.delegate = context.coordinator
        textField.backgroundColor = .clear
        textField.borderStyle = .none
        textField.returnKeyType = .send
        textField.textColor = textColor
        textField.tintColor = UIColor(red: 0.306, green: 0.804, blue: 0.769, alpha: 1)
        textField.font = UIFont.roundedSystemFont(ofSize: 17, weight: .regular)
        textField.autocorrectionType = .yes
        textField.autocapitalizationType = .sentences
        textField.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        textField.addTarget(context.coordinator, action: #selector(Coordinator.textDidChange(_:)), for: .editingChanged)
        updatePlaceholder(for: textField)
        return textField
    }

    func updateUIView(_ textField: UITextField, context: Context) {
        context.coordinator.parent = self

        if textField.text != text {
            textField.text = text
        }

        updatePlaceholder(for: textField)

        if context.coordinator.lastFocusRequestID != focusRequestID {
            context.coordinator.lastFocusRequestID = focusRequestID
            DispatchQueue.main.async {
                textField.becomeFirstResponder()
            }
        } else if isFocused && !textField.isFirstResponder {
            DispatchQueue.main.async {
                textField.becomeFirstResponder()
            }
        } else if !isFocused && textField.isFirstResponder {
            textField.resignFirstResponder()
        }

        textField.textColor = textColor
    }

    func makeCoordinator() -> Coordinator {
        Coordinator(parent: self)
    }

    private func updatePlaceholder(for textField: UITextField) {
        textField.attributedPlaceholder = NSAttributedString(
            string: placeholder,
            attributes: [
                .foregroundColor: placeholderColor,
                .font: UIFont.roundedSystemFont(ofSize: 17, weight: .regular)
            ]
        )
    }

    final class Coordinator: NSObject, UITextFieldDelegate {
        var parent: HomeAIComposerTextField
        var lastFocusRequestID: Int

        init(parent: HomeAIComposerTextField) {
            self.parent = parent
            self.lastFocusRequestID = parent.focusRequestID
        }

        @objc func textDidChange(_ textField: UITextField) {
            parent.text = textField.text ?? ""
        }

        func textFieldDidBeginEditing(_ textField: UITextField) {
            parent.isFocused = true
        }

        func textFieldDidEndEditing(_ textField: UITextField) {
            parent.isFocused = false
        }

        func textFieldShouldReturn(_ textField: UITextField) -> Bool {
            parent.onSubmit()
            return false
        }
    }
}

private extension UIFont {
    static func roundedSystemFont(ofSize size: CGFloat, weight: UIFont.Weight) -> UIFont {
        let systemFont = UIFont.systemFont(ofSize: size, weight: weight)
        guard let descriptor = systemFont.fontDescriptor.withDesign(.rounded) else {
            return systemFont
        }
        return UIFont(descriptor: descriptor, size: size)
    }
}

private struct AIThinkingDots: View {
    @State private var phase = 0

    var body: some View {
        HStack(spacing: 4) {
            ForEach(0..<3, id: \.self) { i in
                Circle()
                    .fill(Color(.secondaryLabel))
                    .frame(width: 5, height: 5)
                    .scaleEffect(phase == i ? 1.4 : 1.0)
                    .animation(.easeInOut(duration: 0.4).repeatForever(autoreverses: true).delay(Double(i) * 0.15), value: phase)
            }
        }
        .onAppear {
            phase = 0
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.05) { phase = 2 }
        }
    }
}

private struct HomeAIMarkdownText: View {
    let text: String

    var body: some View {
        Markdown(text)
        .markdownTheme(.gitHub)
        .markdownTextStyle(\.text) {
            FontSize(.em(1.0))
            ForegroundColor(Color(.label))
            BackgroundColor(nil)
        }
        .markdownTextStyle(\.strong) {
            FontWeight(.semibold)
        }
        .markdownTextStyle(\.code) {
            FontFamilyVariant(.monospaced)
            FontSize(.em(0.92))
            BackgroundColor(nil)
        }
        .markdownBlockStyle(\.paragraph) { configuration in
            configuration.label
                .relativeLineSpacing(.em(0.22))
                .markdownMargin(top: 0, bottom: 14)
        }
        .markdownBlockStyle(\.codeBlock) { configuration in
            ScrollView(.horizontal, showsIndicators: false) {
                configuration.label
                    .padding(.horizontal, 12)
                    .padding(.vertical, 10)
            }
            .background(Color.clear)
            .overlay(
                RoundedRectangle(cornerRadius: 12, style: .continuous)
                    .stroke(Color(hex: "4A5240").opacity(0.22), lineWidth: 1)
            )
            .markdownMargin(top: 6, bottom: 16)
        }
        .markdownBlockStyle(\.blockquote) { configuration in
            configuration.label
                .padding(.leading, 14)
                .padding(.vertical, 2)
                .markdownTextStyle {
                    ForegroundColor(Color(.secondaryLabel))
                }
                .overlay(alignment: .leading) {
                    RoundedRectangle(cornerRadius: 999, style: .continuous)
                        .fill(Color(hex: "4A5240").opacity(0.45))
                        .frame(width: 3)
                }
                .markdownMargin(top: 4, bottom: 16)
        }
        .tint(Color(hex: "4A5240"))
        .textSelection(.enabled)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(Color.clear)
    }
}

private struct HomeAIWorkspacePickerView: View {
    let workspaces: [AIConversationWorkspace]
    let activeWorkspaceID: String
    let activeBucketID: UUID?
    let onSelect: (String) -> Void

    @Environment(\.colorScheme) private var colorScheme
    @Namespace private var animation

    private var darkMode: Bool {
        colorScheme == .dark
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 1) {
            ForEach(workspaces) { workspace in
                HStack(spacing: 10) {
                    Image(systemName: workspace.bucketID == nil ? "sparkles" : "flag.fill")
                        .font(Font.satoshi(13, weight: .semibold))
                        .foregroundColor(workspaceTint(workspace))
                        .frame(width: 22)

                    Text(workspace.title)
                        .lineLimit(1)

                    Spacer()

                    if workspace.id == activeWorkspaceID {
                        Image(systemName: "checkmark")
                            .font(Font.satoshi(14, weight: .medium))
                    }
                }
                .frame(maxWidth: .infinity, alignment: .leading)
                .font(Font.satoshi(18, weight: .medium))
                .padding(5)
                .background {
                    if workspace.id == activeWorkspaceID {
                        RoundedRectangle(cornerRadius: 6, style: .continuous)
                            .fill(darkMode ? Color("AlwaysDarkSecondaryBackground") : Color("AlwaysLightSecondaryBackground"))
                            .matchedGeometryEffect(id: "WORKSPACE", in: animation)
                    }
                }
                .contentShape(Rectangle())
                .onTapGesture {
                    onSelect(workspace.id)
                }
            }
        }
        .foregroundColor(darkMode ? Color("AlwaysLightBackground") : Color("AlwaysDarkBackground"))
        .padding(4)
        .frame(width: 200)
        .background(
            RoundedRectangle(cornerRadius: 9, style: .continuous)
                .fill(darkMode ? Color("AlwaysDarkBackground") : Color("AlwaysLightBackground"))
                .shadow(color: darkMode ? Color.clear : Color.gray.opacity(0.25), radius: 6)
        )
        .overlay(
            RoundedRectangle(cornerRadius: 9, style: .continuous)
                .stroke(darkMode ? Color.gray.opacity(0.1) : Color.clear, lineWidth: 1.3)
        )
    }

    private func workspaceTint(_ workspace: AIConversationWorkspace) -> Color {
        if workspace.bucketID == nil {
            return Color(hex: "4A5240")
        }

        if workspace.bucketID == activeBucketID {
            return Color(hex: "4A5240")
        }

        return Color(hex: "4A5240")
    }
}

private struct ThinkingDisclosure: View {
    let thinking: String
    @State private var isExpanded = false

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            HStack(spacing: 6) {
                Image(systemName: "brain")
                    .font(Font.satoshi(11, weight: .semibold))
                    .foregroundStyle(
                        LinearGradient(
                            colors: [Color(hex: "4A5240"), Color(hex: "C8B94A")],
                            startPoint: .leading, endPoint: .trailing
                        )
                    )
                Text("Reasoning")
                    .font(Font.satoshi(.caption, weight: .semibold))
                    .foregroundColor(Color(.secondaryLabel))
                Spacer(minLength: 0)
                Image(systemName: isExpanded ? "chevron.up" : "chevron.down")
                    .font(Font.satoshi(10, weight: .medium))
                    .foregroundColor(Color(.tertiaryLabel))
            }
            .padding(.horizontal, 10)
            .padding(.vertical, 8)
            .contentShape(Rectangle())
            .onTapGesture {
                withAnimation(.spring(response: 0.28, dampingFraction: 0.8)) {
                    isExpanded.toggle()
                }
            }

            if isExpanded {
                Rectangle()
                    .fill(Color(.separator).opacity(0.4))
                    .frame(height: 1)
                    .padding(.horizontal, 10)

                Text(thinking)
                    .font(Font.satoshi(.caption))
                    .foregroundColor(Color(.secondaryLabel))
                    .fixedSize(horizontal: false, vertical: true)
                    .padding(10)
            }
        }
        .background(
            RoundedRectangle(cornerRadius: 10, style: .continuous)
                .fill(Color(.tertiarySystemBackground))
                .overlay(
                    RoundedRectangle(cornerRadius: 10, style: .continuous)
                        .strokeBorder(
                            LinearGradient(
                                colors: [Color(hex: "4A5240").opacity(0.35), Color(hex: "C8B94A").opacity(0.35)],
                                startPoint: .leading, endPoint: .trailing
                            ),
                            lineWidth: 1
                        )
                )
        )
        .clipShape(RoundedRectangle(cornerRadius: 10, style: .continuous))
    }
}

private struct SakuAILandingAction: Identifiable {
    let id = UUID()
    let title: String
    let systemImage: String
    let prompt: String?
}

struct HomeAIAnimatedGradientBackground: View {
    var body: some View {
        ZStack {
            RadialGradient(
                colors: [Color(hex: "4A5240").opacity(0.28), Color.clear],
                center: .topLeading,
                startRadius: 0,
                endRadius: 340
            )
            RadialGradient(
                colors: [Color(hex: "C8B94A").opacity(0.22), Color.clear],
                center: .topTrailing,
                startRadius: 0,
                endRadius: 320
            )
        }
    }
}
